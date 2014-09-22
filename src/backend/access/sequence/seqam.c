/*-------------------------------------------------------------------------
 *
 * seqam.c
 *	  general sequence access method routines
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/sequence/seqam.c
 *
 *
 * Sequence access method allows the SQL Standard Sequence objects to be
 * managed according to either the default access method or a pluggable
 * replacement. Each sequence can only use one access method at a time,
 * though different sequence access methods can be in use by different
 * sequences at the same time.
 *
 * The SQL Standard assumes that each Sequence object is completely controlled
 * from the current database node, preventing any form of clustering mechanisms
 * from controlling behaviour. Sequence access methods are general purpose
 * though designed specifically to address the needs of Sequences working as
 * part of a multi-node "cluster", though that is not defined here, nor are
 * there dependencies on anything outside of this module, nor any particular
 * form of clustering.
 *
 * The SQL Standard behaviour, also the historical PostgreSQL behaviour, is
 * referred to as the "Local" SeqAM. That is also the basic default.  Local
 * SeqAM assumes that allocations from the sequence will be contiguous, so if
 * user1 requests a range of values and is given 500-599 as values for their
 * backend then the next user to make a request will be given a range starting
 * with 600.
 *
 * The SeqAM mechanism allows us to override the Local behaviour, for use with
 * clustering systems. When multiple masters can request ranges of values it
 * would break the assumption of contiguous allocation. It seems likely that
 * the SeqAM would also wish to control node-level caches for sequences to
 * ensure good performance. The CACHE option and other options may be
 * overridden by the _init API call, if needed, though in general having
 * cacheing per backend and per node seems desirable.
 *
 * SeqAM allows calls to allocate a new range of values, reset the sequence to
 * a new value and to define options for the AM module. The on-disk format of
 * Sequences is the same for all AMs, except that each sequence has a SeqAm
 * defined private-data column, amdata.
 *
 * SeqAMs work similarly to IndexAMs in many ways. pg_class.relam stores the
 * Oid of the SeqAM, just as we do for IndexAm. The relcache stores AM
 * information in much the same way for indexes and sequences, and management
 * of options is similar also.
 *
 * Note that the SeqAM API calls are synchronous. It is up to the SeqAM to
 * decide how that is handled, for example, whether there is a higher level
 * cache at instance level to amortise network traffic in cluster.
 *
 * The SeqAM is identified by Oid of corresponding tuple in pg_seqam.  There is
 * no syscache for pg_seqam, though the SeqAM data is stored on the relcache
 * entry for the sequence.
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/seqam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_seqam.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/syscache.h"

char	*serial_seqam = NULL;

#define GET_SEQAM_PROCEDURE(pname, missing_ok) \
do { \
	procedure = &seqrel->rd_seqaminfo->pname; \
	if (!OidIsValid(procedure->fn_oid)) \
	{ \
		RegProcedure	procOid = seqrel->rd_seqam->pname; \
		if (RegProcedureIsValid(procOid)) \
			fmgr_info_cxt(procOid, procedure, seqrel->rd_indexcxt); \
		else if (!missing_ok) \
			elog(ERROR, "invalid %s regproc", CppAsString(pname)); \
	} \
} while(0)

/*-------------------------------------------------------------------------
 *
 *  Sequence Access Method API
 *
 *  INTERFACE ROUTINES
 *		seqam_init			- initialize sequence, also used for resetting
 *		seqam_alloc			- allocate a new range of values for the sequence
 *		seqam_setval		- implements the setval SQL interface
 *		seqam_get_state		- dump sequence state (for pg_dump)
 *		seqam_set_state		- restore sequence state (for pg_dump)
 *
 * Additionally, the am_reloptions interface is used for parsing reloptions
 * that can be used for passing AM specific options.
 *
 *-------------------------------------------------------------------------
 */

/*
 * seqam_init - initialize/replace custom sequence am values
 */
HeapTuple
seqam_init(Relation seqrel, HeapTuple tuple, int64 restart_value,
		   bool restart_requested, bool is_init)
{
	FmgrInfo	procedure;
	Datum		ret;

	fmgr_info(seqrel->rd_seqam->seqaminit, &procedure);

	/*
	 * Have the seqam's proc do its work.
	 */
	ret = FunctionCall5(&procedure,
						PointerGetDatum(seqrel),
						PointerGetDatum(tuple),
						Int64GetDatum(restart_value),
						BoolGetDatum(restart_requested),
						BoolGetDatum(is_init));

	return (HeapTuple) DatumGetPointer(ret);
}

/*
 * seqam_alloc - allocate sequence values in a sequence
 */
int64
seqam_alloc(Relation seqrel, SequenceHandle *seqh, int64 nrequested,
			int64 *last)
{
	FmgrInfo   *procedure;
	Datum		ret;

	Assert(RelationIsValid(seqrel));
	Assert(PointerIsValid(seqrel->rd_seqam));
	Assert(OidIsValid(seqrel->rd_rel->relam));

	GET_SEQAM_PROCEDURE(seqamalloc, false);

	/*
	 * have the seqam's alloc proc do its work.
	 */
	ret = FunctionCall4(procedure,
						PointerGetDatum(seqrel),
						PointerGetDatum(seqh),
						Int64GetDatum(nrequested),
						PointerGetDatum(last));
	return DatumGetInt64(ret);
}

/*
 * seqam_setval - set sequence values in a sequence
 */
void
seqam_setval(Relation seqrel, SequenceHandle *seqh, int64 new_value)
{
	FmgrInfo   *procedure;

	Assert(RelationIsValid(seqrel));
	Assert(PointerIsValid(seqrel->rd_seqam));
	Assert(OidIsValid(seqrel->rd_rel->relam));

	GET_SEQAM_PROCEDURE(seqamsetval, true);

	if (!OidIsValid(procedure->fn_oid))
		return;

	/*
	 * have the seqam's setval proc do its work.
	 */
	FunctionCall3(procedure,
				  PointerGetDatum(seqrel),
				  PointerGetDatum(seqh),
				  Int64GetDatum(new_value));
}

/*
 * seqam_get_state - pg_dump support
 */
ArrayType *
seqam_get_state(Relation seqrel, SequenceHandle *seqh)
{
	FmgrInfo	procedure;
	Datum		statearr;

	Assert(RelationIsValid(seqrel));
	Assert(PointerIsValid(seqrel->rd_seqam));
	Assert(OidIsValid(seqrel->rd_rel->relam));

	fmgr_info(seqrel->rd_seqam->seqamgetstate, &procedure);

	/*
	 * have the seqam's setval proc do its work.
	 */
	statearr = FunctionCall2(&procedure,
							 PointerGetDatum(seqrel),
							 PointerGetDatum(seqh));

	return (ArrayType *) DatumGetPointer(statearr);
}

/*
 * seqam_set_state - restore from pg_dump
 */
void
seqam_set_state(Relation seqrel, SequenceHandle *seqh, ArrayType *statearr)
{
	FmgrInfo	procedure;

	Assert(RelationIsValid(seqrel));
	Assert(PointerIsValid(seqrel->rd_seqam));
	Assert(OidIsValid(seqrel->rd_rel->relam));

	fmgr_info(seqrel->rd_seqam->seqamsetstate, &procedure);

	/*
	 * have the seqam's setval proc do its work.
	 */
	FunctionCall3(&procedure,
				  PointerGetDatum(seqrel),
				  PointerGetDatum(seqh),
				  PointerGetDatum(statearr));
}


/*------------------------------------------------------------
 *
 * Sequence Access Manager management functions
 *
 *------------------------------------------------------------
 */

/* check_hook: validate new serial_seqam value */
bool
check_serial_seqam(char **newval, void **extra, GucSource source)
{
	/*
	 * If we aren't inside a transaction, we cannot do database access so
	 * cannot verify the name.  Must accept the value on faith.
	 */
	if (IsTransactionState())
	{
		if (!OidIsValid(get_seqam_oid(*newval, true)))
		{
			/*
			 * When source == PGC_S_TEST, we are checking the argument of an
			 * ALTER DATABASE SET or ALTER USER SET command.  Value may
			 * be created later.  Because of that, issue a NOTICE if source ==
			 * PGC_S_TEST, but accept the value anyway.
			 */
			if (source == PGC_S_TEST)
			{
				ereport(NOTICE,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("sequence access method \"%s\" does not exist",
								*newval)));
			}
			else
			{
				GUC_check_errdetail("sequence access method \"%s\" does not exist.",
									*newval);
				return false;
			}
		}
	}
	return true;
}


/*
 * get_seqam_oid - given a sequence AM name, look up the OID
 *
 * If missing_ok is false, throw an error if SeqAM name not found.  If true,
 * just return InvalidOid.
 */
Oid
get_seqam_oid(const char *amname, bool missing_ok)
{
	Oid			result;
	HeapTuple	tuple;

	/* look up the access method */
	tuple = SearchSysCache1(SEQAMNAME, PointerGetDatum(amname));

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
	{
		result = HeapTupleGetOid(tuple);
		ReleaseSysCache(tuple);
	}
	else
		result = InvalidOid;

	if (!OidIsValid(result) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("sequence access method \"%s\" does not exist",
						amname)));
	return result;
}
