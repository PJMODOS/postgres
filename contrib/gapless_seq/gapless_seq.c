/*-------------------------------------------------------------------------
 *
 * gapless_seq.c
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/gapless_seq/gapless_seq.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/seqam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/sequence.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "storage/procarray.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"


PG_MODULE_MAGIC;

/*------------------------------------------------------------
 *
 * Sequence Access Manager = Gapless functions
 *
 *------------------------------------------------------------
 */
extern Datum seqam_gapless_reloptions(PG_FUNCTION_ARGS);
extern Datum seqam_gapless_init(PG_FUNCTION_ARGS);
extern Datum seqam_gapless_alloc(PG_FUNCTION_ARGS);
extern Datum seqam_gapless_setval(PG_FUNCTION_ARGS);
extern Datum seqam_gapless_get_state(PG_FUNCTION_ARGS);
extern Datum seqam_gapless_set_state(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(seqam_gapless_reloptions);
PG_FUNCTION_INFO_V1(seqam_gapless_init);
PG_FUNCTION_INFO_V1(seqam_gapless_alloc);
PG_FUNCTION_INFO_V1(seqam_gapless_setval);
PG_FUNCTION_INFO_V1(seqam_gapless_get_state);
PG_FUNCTION_INFO_V1(seqam_gapless_set_state);

typedef struct GaplessAmdata
{
	uint32		xid;
} GaplessAmdata;

typedef struct GaplessValue
{
	Oid		seqid;
	bool	is_called;
	int64	last_value;
} GaplessValue;

#define GAPLESS_SEQ_NAMESPACE "gapless_seq"
#define VALUES_TABLE_NAME "seqam_gapless_values"
#define VALUES_TABLE_COLUMNS 3

static FormData_pg_sequence *wait_for_sequence(SequenceHandle *seqh,
											   TransactionId local_xid);
static Relation open_values_rel(void);
static HeapTuple get_last_value_tup(Relation rel, Oid seqid);
static void set_last_value_tup(Relation rel, Oid seqid, int64 last_value,
							   bool is_called, HeapTuple oldtuple);


/*
 * seqam_gapless_reloptions()
 *
 * Parse and verify the reloptions of a gapless sequence.
 */
Datum
seqam_gapless_reloptions(PG_FUNCTION_ARGS)
{
	Datum		reloptions = PG_GETARG_DATUM(0);
	bool		validate = PG_GETARG_BOOL(1);

	if (validate && PointerIsValid(DatumGetPointer(reloptions)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gapless sequence does not accept any storage parameters")));

	PG_RETURN_NULL();
}

/*
 * seqam_gapless_init()
 *
 * Initialize gapless sequence
 *
 */
Datum
seqam_gapless_init(PG_FUNCTION_ARGS)
{
	Relation	seqrel = (Relation) PG_GETARG_POINTER(0);
	HeapTuple	tuple = (HeapTuple) PG_GETARG_POINTER(1);
	int64		restart_value = (int64) PG_GETARG_INT64(2);
	bool		restart_requested = (bool) PG_GETARG_BOOL(3);
	bool		is_init = (bool) PG_GETARG_BOOL(4);
	Oid			seqrelid = seqrel->rd_id;
	TupleDesc	tupDesc = RelationGetDescr(seqrel);
	Datum		amdata;
	bool		isnull;
	Relation	valrel;
	HeapTuple	valtuple;
	TransactionId		local_xid = GetTopTransactionId();
	GaplessAmdata	   *gapless_seq;

	/* Load current value if this is existing sequence. */
	valrel = open_values_rel();
	valtuple = get_last_value_tup(valrel, seqrelid);

	/*
	 * If this is new sequence or restart was provided or if there is
	 * no previous stored value for the sequence we should store it in
	 * the values table.
	 */
	if (is_init || restart_requested || !HeapTupleIsValid(valtuple))
		set_last_value_tup(valrel, seqrelid, restart_value, false, valtuple);

	/* Now we are done with values relation, but keep the lock. */
	heap_close(valrel, NoLock);

	amdata = fastgetattr(tuple, SEQ_COL_AMDATA, tupDesc,
						 &isnull);

	/* Make sure amdata is filled. */
	if (isnull)
	{
		Datum			values[SEQ_COL_LASTCOL];
		bool			nulls[SEQ_COL_LASTCOL];
		bool			replace[SEQ_COL_LASTCOL];
		struct varlena *vl = palloc0(VARHDRSZ + sizeof(GaplessAmdata));
		HeapTuple		newtup;

		memset(replace, false, sizeof(replace));
		memset(nulls, false, sizeof(nulls));
		memset(values, 0, sizeof(values));

		replace[SEQ_COL_AMDATA - 1] = true;
		nulls[SEQ_COL_AMDATA - 1] = false;

		SET_VARSIZE(vl, VARHDRSZ + sizeof(GaplessAmdata));
		values[SEQ_COL_AMDATA - 1] = PointerGetDatum(vl);

		newtup = heap_modify_tuple(tuple, tupDesc, values, nulls, replace);

		/* Don't leak memory. */
		heap_freetuple(tuple);
		tuple = newtup;

		amdata = fastgetattr(tuple, SEQ_COL_AMDATA, tupDesc,
							 &isnull);
	}

	gapless_seq = (GaplessAmdata *)
		VARDATA_ANY(DatumGetByteaP(amdata));

	/* Update the xid info */
	gapless_seq->xid = UInt32GetDatum(local_xid);

	PG_RETURN_POINTER(tuple);
}

/*
 * seqam_gapless_alloc()
 *
 * Allocate new value for gapless sequence.
 */
Datum
seqam_gapless_alloc(PG_FUNCTION_ARGS)
{
	Relation	seqrel = (Relation) PG_GETARG_POINTER(0);
	SequenceHandle *seqh = (SequenceHandle*) PG_GETARG_POINTER(1);
	/* we ignore nreguested as gapless sequence can't do caching */
	int64	   *last = (int64 *) PG_GETARG_POINTER(3);
	int64		result;
	Oid			seqrelid = RelationGetRelid(seqrel);
	Relation	valrel;
	HeapTuple	tuple;
	FormData_pg_sequence   *seq;
	GaplessAmdata	   *gapless_seq;
	TransactionId local_xid = GetTopTransactionId();

	/* Wait until the sequence is locked by us. */
	seq = wait_for_sequence(seqh, local_xid);
	gapless_seq = (GaplessAmdata *) VARDATA_ANY(&seq->amdata);

	/* Read the last value from our transactional table (if any). */
	valrel = open_values_rel();
	tuple = get_last_value_tup(valrel, seqrelid);

	/* Last value found get next value. */
	if (HeapTupleIsValid(tuple))
	{
		GaplessValue *v = (GaplessValue *) GETSTRUCT(tuple);
		result = v->last_value;

		if (v->is_called)
			(void) sequence_increment(seqrel, &result, 1, seq->min_value,
									  seq->max_value,
									  seq->increment_by,
									  seq->is_cycled, true);
	}
	else /* No last value, start from beginning. */
		result = seq->start_value;

	/*
	 * Insert or update the last value tuple.
	 */
	set_last_value_tup(valrel, seqrelid, result, true, tuple);

	/* Now we are done with values relation, but keep the lock. */
	heap_close(valrel, NoLock);

	/*
	 * If current tx is different fron the last one,
	 * update the sequence tuple as well.
	 *
	 * We don't need to WAL log the update as the only thing we save to
	 * sequence tuple is the active transaction id and we know that in case of
	 * crash the transaction id will not be active so it's ok to lose the
	 * update.
	 */
	if (gapless_seq->xid != local_xid)
	{
		sequence_start_update(seqh, true);
		gapless_seq->xid = local_xid;
		sequence_apply_update(seqh, true);
		sequence_finish_update(seqh);
	}

	*last = result;
	PG_RETURN_INT64(result);
}

/*
 * seqam_gapless_setval()
 *
 * Setval support (we don't allow setval on gapless)
 */
Datum
seqam_gapless_setval(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("setval() is not supported for gapless sequences")));

	PG_RETURN_VOID();
}

/*
 * seqam_gapless_get_state()
 *
 * Dump state of a gapless sequence (for pg_dump)
 */
Datum
seqam_gapless_get_state(PG_FUNCTION_ARGS)
{
	Relation		seqrel = (Relation) PG_GETARG_POINTER(0);
	SequenceHandle *seqh = (SequenceHandle *) PG_GETARG_POINTER(1);
	Datum			datums[4];
	Datum			val;
	Oid				seqrelid = RelationGetRelid(seqrel);
	Relation		valrel;
	HeapTuple		tuple;
	int64			last_value;
	bool			is_called;

	/*
	 * Get the last value from the values table, if not found use start_value
	 * from the sequence definition.
	 */
	valrel = open_values_rel();
	tuple = get_last_value_tup(valrel, seqrelid);
	heap_close(valrel, RowExclusiveLock);

	if (HeapTupleIsValid(tuple))
	{
		GaplessValue *v = (GaplessValue *) GETSTRUCT(tuple);
		last_value = v->last_value;
		is_called = v->is_called;
	}
	else
	{
		FormData_pg_sequence *seq = (FormData_pg_sequence *)
			GETSTRUCT(sequence_read_tuple(seqh));
		last_value = seq->start_value;
		is_called = false;
		sequence_release_tuple(seqh);
	}

	datums[0] = CStringGetTextDatum("last_value");
	val = DirectFunctionCall1(int8out, Int64GetDatum(last_value));
	datums[1] = CStringGetTextDatum(DatumGetCString(val));

	datums[2] = CStringGetTextDatum("is_called");
	val = DirectFunctionCall1(boolout, BoolGetDatum(is_called));
	datums[3] = CStringGetTextDatum(DatumGetCString(val));

	PG_RETURN_ARRAYTYPE_P(construct_array(datums, 4, TEXTOID, -1, false, 'i'));
}

/*
 * seqam_gapless_set_state()
 *
 * Restore previously dumpred state of gapless sequence
 */
Datum
seqam_gapless_set_state(PG_FUNCTION_ARGS)
{
	Relation		seqrel = (Relation) PG_GETARG_POINTER(0);
	SequenceHandle *seqh = (SequenceHandle*) PG_GETARG_POINTER(1);
	ArrayType	   *array = PG_GETARG_ARRAYTYPE_P(2);
	Oid				seqrelid = RelationGetRelid(seqrel);
	Relation		valrel;
	HeapTuple		tuple;
	int64			last_value = 0;
	bool			is_called = false;
	bool			last_value_found = false,
					is_called_found = false;
	Datum		   *datums;
	int				count;
	int				i;
	FormData_pg_sequence   *seq;
	GaplessAmdata	   *gapless_seq;
	TransactionId local_xid = GetTopTransactionId();

	Assert(ARR_ELEMTYPE(array) == TEXTOID && ARR_NDIM(array) == 1 &&
		   (ARR_DIMS(array)[0]) % 2 == 0 && !ARR_HASNULL(array));

	deconstruct_array(array,
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
					 errmsg("invalid state key \"%s\" for gapless sequence",
							key)));
	}

	if (!last_value_found)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("last_value is required parameter for gapless sequence")));

	if (!is_called_found)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("is_called is required parameter for gapless sequence")));

	/* Wait until the sequence is locked by us. */
	seq = wait_for_sequence(seqh, local_xid);
	gapless_seq = (GaplessAmdata *) VARDATA_ANY(&seq->amdata);

	sequence_check_range(last_value, seq->min_value, seq->max_value, "last_value");

	/* Read the last value from our transactional table (if any). */
	valrel = open_values_rel();
	tuple = get_last_value_tup(valrel, seqrelid);

	/* Insert or update the last value tuple. */
	set_last_value_tup(valrel, seqrelid, last_value, is_called, tuple);

	/* Now we are done with values relation, but keep the lock. */
	heap_close(valrel, NoLock);

	/* Save to updated sequence. */
	sequence_start_update(seqh, true);
	gapless_seq->xid = local_xid;
	sequence_apply_update(seqh, true);
	sequence_finish_update(seqh);
	sequence_release_tuple(seqh);

	PG_FREE_IF_COPY(array, 2);

	PG_RETURN_VOID();
}

/*
 * Lock the sequence for current transaction.
 */
static FormData_pg_sequence *
wait_for_sequence(SequenceHandle *seqh, TransactionId local_xid)
{
	FormData_pg_sequence   *seq = (FormData_pg_sequence *) GETSTRUCT(sequence_read_tuple(seqh));
	GaplessAmdata	   *gapless_seq = (GaplessAmdata *) VARDATA_ANY(&seq->amdata);

	/*
	 * Read and lock the sequence for our transaction, there can't be any
	 * concurrent transactions accessing the sequence at the same time.
	 */
	while (gapless_seq->xid != local_xid &&
		   TransactionIdIsInProgress(gapless_seq->xid))
	{
		/*
		 * Release tuple to avoid dead locks and wait for the concurrent tx
		 * to finish.
		 */
		sequence_release_tuple(seqh);
		XactLockTableWait(gapless_seq->xid, NULL, NULL, XLTW_None);
		/* Reread the sequence. */
		seq = (FormData_pg_sequence *) GETSTRUCT(sequence_read_tuple(seqh));
	}

	return seq;
}

/*
 * Open the relation used for storing last value in RowExclusive lock mode.
 */
static Relation
open_values_rel(void)
{
	RangeVar   *rv;
	Oid			valrelid;
	Relation	valrel;

	rv = makeRangeVar(GAPLESS_SEQ_NAMESPACE, VALUES_TABLE_NAME, -1);
	valrelid = RangeVarGetRelid(rv, RowExclusiveLock, false);
	valrel = heap_open(valrelid, RowExclusiveLock);

	return valrel;
}

/*
 * Read the last value tuple from the values table.
 *
 * Can return NULL if tuple is not found.
 */
static HeapTuple
get_last_value_tup(Relation rel, Oid seqid)
{
	ScanKey		key;
	SysScanDesc	scan;
	HeapTuple	tuple;

	key = (ScanKey) palloc(sizeof(ScanKeyData) * 1);

	ScanKeyInit(&key[0],
				1,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(seqid)
		);

	/* FIXME: should use index */
	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
		tuple = heap_copytuple(tuple);
	systable_endscan(scan);

	return tuple;
}

/*
 * Insert or update the last value tuple.
 *
 * The write access to the table must be serialized by the wait_for_sequence
 * so that we don't have to have any retry scheme here.
*/
static void
set_last_value_tup(Relation rel, Oid seqid, int64 last_value, bool is_called, HeapTuple oldtuple)
{
	bool		nulls[VALUES_TABLE_COLUMNS];
	Datum		values[VALUES_TABLE_COLUMNS];
	TupleDesc	tupDesc = RelationGetDescr(rel);
	HeapTuple	tuple;

	if (!HeapTupleIsValid(oldtuple))
	{
		memset(nulls, false, VALUES_TABLE_COLUMNS * sizeof(bool));
		values[0] = ObjectIdGetDatum(seqid);
		values[1] = BoolGetDatum(is_called);
		values[2] = Int64GetDatum(last_value);

		tuple = heap_form_tuple(tupDesc, values, nulls);
		simple_heap_insert(rel, tuple);
	}
	else
	{
		bool replaces[VALUES_TABLE_COLUMNS];

		replaces[0] = false;
		replaces[1] = true;
		replaces[2] = true;

		nulls[1] = false;
		nulls[2] = false;
		values[1] = BoolGetDatum(is_called);
		values[2] = Int64GetDatum(last_value);

		tuple = heap_modify_tuple(oldtuple, tupDesc, values, nulls, replaces);
		simple_heap_update(rel, &tuple->t_self, tuple);
	}

	CatalogUpdateIndexes(rel, tuple);
}
