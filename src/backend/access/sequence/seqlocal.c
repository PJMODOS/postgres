/*-------------------------------------------------------------------------
 *
 * seqlocal.c
 *	  Local sequence access manager
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *        src/backend/access/sequence/seqlocal.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/seqam.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/rel.h"

/*
 * We don't want to log each fetching of a value from a sequence,
 * so we pre-log a few fetches in advance. In the event of
 * crash we can lose (skip over) as many values as we pre-logged.
 */
#define SEQ_LOG_VALS	32

/* Definition of additional columns for local sequence. */
typedef struct LocalAmdata
{
	int64           log_cnt;
} LocalAmdata;


/*
 * seqam_local_reloptions()
 *
 * Parse and verify the reloptions of a local sequence.
 */
Datum
seqam_local_reloptions(PG_FUNCTION_ARGS)
{
	Datum		reloptions = PG_GETARG_DATUM(0);
	bool		validate = PG_GETARG_BOOL(1);

	if (validate && PointerIsValid(DatumGetPointer(reloptions)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("local sequence does not accept any storage parameters")));

	PG_RETURN_NULL();
}

/*
 * seqam_local_init()
 *
 * Initialize local sequence
 */
Datum
seqam_local_init(PG_FUNCTION_ARGS)
{
	Relation	seqrel = (Relation) PG_GETARG_POINTER(0);
	HeapTuple	tuple = (HeapTuple) PG_GETARG_POINTER(1);
	int64		restart_value = PG_GETARG_INT64(2);
	bool		restart_requested = PG_GETARG_BOOL(3);
	bool		is_init = PG_GETARG_BOOL(4);
	bool		isnull;
	Datum		amdata;
	TupleDesc	tupDesc = RelationGetDescr(seqrel);
	Form_pg_sequence seq = (Form_pg_sequence) GETSTRUCT(tuple);
	LocalAmdata *localseq;

	/*
	 * If this is a new sequence or RESTART was provided in ALTER we should
	 * reset our state to that new starting point.
	 */
	if (is_init || restart_requested)
	{
		seq->last_value = restart_value;
		seq->is_called = false;
	}

	amdata = fastgetattr(tuple, SEQ_COL_AMDATA, tupDesc,
						 &isnull);

	/* Make sure amdata is filled. */
	if (isnull)
	{
		Datum			values[SEQ_COL_LASTCOL];
		bool			nulls[SEQ_COL_LASTCOL];
		bool			replace[SEQ_COL_LASTCOL];
		struct varlena *vl = palloc0(VARHDRSZ + sizeof(LocalAmdata));
		HeapTuple		newtup;

		memset(replace, false, sizeof(replace));
		memset(nulls, false, sizeof(nulls));
		memset(values, 0, sizeof(values));

		replace[SEQ_COL_AMDATA - 1] = true;
		nulls[SEQ_COL_AMDATA - 1] = false;

		SET_VARSIZE(vl, VARHDRSZ + sizeof(LocalAmdata));
		values[SEQ_COL_AMDATA - 1] = PointerGetDatum(vl);

		newtup = heap_modify_tuple(tuple, tupDesc, values, nulls, replace);

		/* Don't leak memory. */
		heap_freetuple(tuple);
		tuple = newtup;

		amdata = fastgetattr(tuple, SEQ_COL_AMDATA, tupDesc,
							 &isnull);
	}

	localseq = (LocalAmdata *)
		VARDATA_ANY(DatumGetByteaP(amdata));

	/* We always reset the log_cnt. */
	localseq->log_cnt = 0;

	PG_RETURN_POINTER(tuple);
}

/*
 * seqam_local_alloc()
 *
 * Allocate a new range of values for a local sequence.
 */
Datum
seqam_local_alloc(PG_FUNCTION_ARGS)
{
	Relation	seqrel = (Relation) PG_GETARG_POINTER(0);
	SequenceHandle *seqh = (SequenceHandle *) PG_GETARG_POINTER(1);
	int64	    nrequested = PG_GETARG_INT64(2);
	int64	   *last = (int64 *) PG_GETARG_POINTER(3);
	FormData_pg_sequence *seq;
	int64		incby,
				maxv,
				minv,
				log,
				fetch,
				result,
				next,
				rescnt = 0;
	bool		is_cycled,
				is_called,
				logit = false;
	LocalAmdata *localseq;

	seq = (FormData_pg_sequence *) GETSTRUCT(sequence_read_tuple(seqh));
	localseq = (LocalAmdata *) VARDATA_ANY(&seq->amdata);

	next = result = seq->last_value;
	incby = seq->increment_by;
	maxv = seq->max_value;
	minv = seq->min_value;
	is_cycled = seq->is_cycled;
	fetch = nrequested;
	log = localseq->log_cnt;
	is_called = seq->is_called;

	/* We are returning last_value if not is_called so fetch one less value. */
	if (!is_called)
	{
		nrequested--;
		fetch--;
	}

	/*
	 * Decide whether we should emit a WAL log record.  If so, force up the
	 * fetch count to grab SEQ_LOG_VALS more values than we actually need to
	 * cache.  (These will then be usable without logging.)
	 *
	 * If this is the first nextval after a checkpoint, we must force a new
	 * WAL record to be written anyway, else replay starting from the
	 * checkpoint would fail to advance the sequence past the logged values.
	 * In this case we may as well fetch extra values.
	 */
	if (log < fetch || !is_called)
	{
		/* Forced log to satisfy local demand for values. */
		fetch = log = fetch + SEQ_LOG_VALS;
		logit = true;
	}
	else if (sequence_needs_wal(seqh))
	{
		fetch = log = fetch + SEQ_LOG_VALS;
		logit = true;
	}

	/* Fetch new result value if is_called. */
	if (is_called)
	{
		rescnt += sequence_increment(seqrel, &next, 1, minv, maxv, incby,
									 is_cycled, true);
		result = next;
	}

	/* Fetch as many values as was requested by backend. */
	if (rescnt < nrequested)
		rescnt += sequence_increment(seqrel, &next, nrequested - rescnt, minv,
									 maxv, incby, is_cycled, false);

	/* Last value available for calling backend. */
	*last = next;
	/* Values we made available to calling backend can't be counted as cached. */
	log -= rescnt;

	/* We might need to fetch even more values for our own caching. */
	if (rescnt < fetch)
		rescnt += sequence_increment(seqrel, &next, fetch - rescnt, minv,
									 maxv, incby, is_cycled, false);

	fetch -= rescnt;
	log -= fetch;				/* adjust for any unfetched numbers */
	Assert(log >= 0);

	/*
	 * Log our cached data.
	 */
	sequence_start_update(seqh, logit);
	if (logit)
	{
		seq->last_value = next;
		seq->is_called = true;
		localseq->log_cnt = 0;

		sequence_apply_update(seqh, true);
	}

	/* Now update sequence tuple to the intended final state */
	seq->last_value = *last;		/* last fetched number */
	seq->is_called = true;
	localseq->log_cnt = log;		/* how much is logged */

	sequence_apply_update(seqh, false);
	sequence_finish_update(seqh);

	PG_RETURN_INT64(result);
}

/*
 * seqam_local_setval()
 *
 * Set value of a local sequence
 */
Datum
seqam_local_setval(PG_FUNCTION_ARGS)
{
	SequenceHandle *seqh = (SequenceHandle *) PG_GETARG_POINTER(1);
	int64		next = PG_GETARG_INT64(2);
	FormData_pg_sequence *seq;
	LocalAmdata *localseq;

	seq = (FormData_pg_sequence *) GETSTRUCT(sequence_read_tuple(seqh));
	localseq = (LocalAmdata *) VARDATA_ANY(&seq->amdata);

	seq->last_value = next;		/* last fetched number */
	seq->is_called = true;
	localseq->log_cnt = 0;		/* how much is logged */

	sequence_start_update(seqh, true);
	sequence_apply_update(seqh, true);
	sequence_finish_update(seqh);
	sequence_release_tuple(seqh);

	PG_RETURN_VOID();
}

/*
 * seqam_local_get_state()
 *
 * Dump state of a local sequence (for pg_dump)
 */
Datum
seqam_local_get_state(PG_FUNCTION_ARGS)
{
	SequenceHandle *seqh = (SequenceHandle *) PG_GETARG_POINTER(1);
	Datum			datums[4];
	Datum			val;
	FormData_pg_sequence *seq;

	seq = (FormData_pg_sequence *) GETSTRUCT(sequence_read_tuple(seqh));

	datums[0] = CStringGetTextDatum("last_value");
	val = DirectFunctionCall1(int8out, Int64GetDatum(seq->last_value));
	datums[1] = CStringGetTextDatum(DatumGetCString(val));

	datums[2] = CStringGetTextDatum("is_called");
	val = DirectFunctionCall1(boolout, BoolGetDatum(seq->is_called));
	datums[3] = CStringGetTextDatum(DatumGetCString(val));

	sequence_release_tuple(seqh);

	PG_RETURN_ARRAYTYPE_P(construct_array(datums, 4, TEXTOID, -1, false, 'i'));
}

/*
 * seqam_local_set_state()
 *
 * Restore previously dumped state of local sequence (used by pg_dump)
*/
Datum
seqam_local_set_state(PG_FUNCTION_ARGS)
{
	SequenceHandle *seqh = (SequenceHandle *) PG_GETARG_POINTER(1);
	ArrayType	   *statearr = PG_GETARG_ARRAYTYPE_P(2);
	FormData_pg_sequence *seq;
	int64			last_value = 0;
	bool			is_called = false;
	bool			last_value_found = false,
					is_called_found = false;
	Datum		   *datums;
	int				count;
	int				i;

	Assert(ARR_ELEMTYPE(statearr) == TEXTOID && ARR_NDIM(statearr) == 1 &&
		   (ARR_DIMS(statearr)[0]) % 2 == 0 && !ARR_HASNULL(statearr));

	deconstruct_array(statearr,
					  TEXTOID, -1, false, 'i',
					  &datums, NULL, &count);
	count /= 2;
	for (i = 0; i < count; ++i)
	{
		char   *key,
			   *val;

		key = TextDatumGetCString(datums[i * 2]);
		val = TextDatumGetCString(datums[i * 2 + 1]);

		if (pg_strcasecmp(key, "last_value") == 0)
		{
			last_value = DatumGetInt64(DirectFunctionCall1(int8in,
														CStringGetDatum(val)));
			last_value_found = true;
		}
		else if (pg_strcasecmp(key, "is_called") == 0)
		{
			is_called = DatumGetBool(DirectFunctionCall1(boolin,
														CStringGetDatum(val)));
			is_called_found = true;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid state key \"%s\" for local sequence",
							key)));
	}

	if (!last_value_found)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("last_value is required parameter for local sequence")));

	if (!is_called_found)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("is_called is required parameter for local sequence")));


	seq = (FormData_pg_sequence *) GETSTRUCT(sequence_read_tuple(seqh));
	sequence_check_range(last_value, seq->min_value, seq->max_value, "last_value");

	sequence_start_update(seqh, true);
	seq->last_value = last_value;
	seq->is_called = is_called;
	sequence_apply_update(seqh, true);
	sequence_finish_update(seqh);
	sequence_release_tuple(seqh);

	PG_FREE_IF_COPY(statearr, 2);

	PG_RETURN_VOID();
}
