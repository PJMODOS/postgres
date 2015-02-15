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
#include "catalog/dependency.h"
#include "catalog/objectaccess.h"
#include "catalog/objectaddress.h"
#include "catalog/indexing.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_seqam.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
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
 * Find seqam function by name and validate it.
 */
static Datum
get_seqam_func(DefElem *defel, int attnum)
{
	List	   *funcName = defGetQualifiedName(defel);
	Oid			typeId[6];
	Oid			retTypeId;
	int			nargs;
	Oid			procOid;

	typeId[0] = INTERNALOID;

	switch (attnum)
	{
		case Anum_pg_seqam_seqamreloptions:
			nargs = 2;
			typeId[1] = BOOLOID;
			retTypeId = BYTEAOID;
			break;

		case Anum_pg_seqam_seqaminit:
			nargs = 5;
			typeId[1] = INTERNALOID;
			typeId[2] = INT8OID;
			typeId[3] = BOOLOID;
			typeId[4] = BOOLOID;
			retTypeId = BYTEAOID;
			break;

		case Anum_pg_seqam_seqamalloc:
			nargs = 4;
			typeId[1] = INTERNALOID;
			typeId[2] = INT8OID;
			typeId[3] = INTERNALOID;
			retTypeId = INT8OID;
			break;

		case Anum_pg_seqam_seqamsetval:
			nargs = 3;
			typeId[1] = INTERNALOID;
			typeId[2] = INT8OID;
			retTypeId = VOIDOID;
			break;

		case Anum_pg_seqam_seqamgetstate:
			nargs = 2;
			typeId[1] = INTERNALOID;
			retTypeId = TEXTARRAYOID;
			break;

		case Anum_pg_seqam_seqamsetstate:
			nargs = 3;
			typeId[1] = INTERNALOID;
			typeId[2] = TEXTARRAYOID;
			retTypeId = VOIDOID;
			break;

		default:
			/* should not be here */
			elog(ERROR, "unrecognized attribute for sequence access method: %d",
				 attnum);
			nargs = 0;			/* keep compiler quiet */
	}

	procOid = LookupFuncName(funcName, nargs, typeId, false);
	if (get_func_rettype(procOid) != retTypeId)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("function %s should return type %s",
						func_signature_string(funcName, nargs, NIL, typeId),
						format_type_be(retTypeId))));

	return ObjectIdGetDatum(procOid);
}


/*
 * make pg_depend entries for a new pg_seqam entry
 */
static void
makeSeqAMDependencies(HeapTuple tuple)
{
	Form_pg_seqam	seqam = (Form_pg_seqam) GETSTRUCT(tuple);
	ObjectAddress	myself,
					referenced;


	myself.classId = SeqAccessMethodRelationId;
	myself.objectId = HeapTupleGetOid(tuple);
	myself.objectSubId = 0;

	/* Dependency on extension. */
	recordDependencyOnCurrentExtension(&myself, false);

	/* Dependencies on functions. */
	referenced.classId = ProcedureRelationId;
	referenced.objectSubId = 0;

	referenced.objectId = seqam->seqamreloptions;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	referenced.objectId = seqam->seqaminit;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	referenced.objectId = seqam->seqamalloc;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	referenced.objectId = seqam->seqamsetval;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	referenced.objectId = seqam->seqamgetstate;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	referenced.objectId = seqam->seqamsetstate;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
}

/*
 * Create a sequence access method record in pg_seqam catalog.
 *
 * Only superusers can create a sequence access methods.
 */
Oid
DefineSeqAM(List *names, List* definition)
{
	char	   *seqamname = strVal(linitial(names));
	Oid			seqamoid;
	ListCell   *pl;
	Relation	rel;
	Datum		values[Natts_pg_seqam];
	bool		nulls[Natts_pg_seqam];
	HeapTuple	tuple;

	/* Must be super user. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create sequence access method \"%s\"",
						seqamname),
				 errhint("Must be superuser to create a sequence access method.")));

	/* Must not already exist. */
	seqamoid = get_seqam_oid(seqamname, true);
	if (OidIsValid(seqamoid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("sequence access method \"%s\" already exists",
						seqamname)));

	/* Initialize the values. */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_seqam_seqamname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(seqamname));

	/*
	 * Loop over the definition list and extract the information we need.
	 */
	foreach(pl, definition)
	{
		DefElem    *defel = (DefElem *) lfirst(pl);

		if (pg_strcasecmp(defel->defname, "reloptions") == 0)
		{
			values[Anum_pg_seqam_seqamreloptions - 1] =
				get_seqam_func(defel, Anum_pg_seqam_seqamreloptions);
		}
		else if (pg_strcasecmp(defel->defname, "init") == 0)
		{
			values[Anum_pg_seqam_seqaminit - 1] =
				get_seqam_func(defel, Anum_pg_seqam_seqaminit);
		}
		else if (pg_strcasecmp(defel->defname, "alloc") == 0)
		{
			values[Anum_pg_seqam_seqamalloc - 1] =
				get_seqam_func(defel, Anum_pg_seqam_seqamalloc);
		}
		else if (pg_strcasecmp(defel->defname, "setval") == 0)
		{
			values[Anum_pg_seqam_seqamsetval - 1] =
				get_seqam_func(defel, Anum_pg_seqam_seqamsetval);
		}
		else if (pg_strcasecmp(defel->defname, "getstate") == 0)
		{
			values[Anum_pg_seqam_seqamgetstate - 1] =
				get_seqam_func(defel, Anum_pg_seqam_seqamgetstate);
		}
		else if (pg_strcasecmp(defel->defname, "setstate") == 0)
		{
			values[Anum_pg_seqam_seqamsetstate - 1] =
				get_seqam_func(defel, Anum_pg_seqam_seqamsetstate);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("sequence access method parameter \"%s\" not recognized",
						defel->defname)));
	}

	/*
	 * Validation.
	 */
	if (!OidIsValid(DatumGetObjectId(values[Anum_pg_seqam_seqamreloptions - 1])))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("sequence access method reloptions function is required")));

	if (!OidIsValid(DatumGetObjectId(values[Anum_pg_seqam_seqaminit - 1])))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("sequence access method init function is required")));

	if (!OidIsValid(DatumGetObjectId(values[Anum_pg_seqam_seqamalloc - 1])))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("sequence access method alloc function is required")));

	if (!OidIsValid(DatumGetObjectId(values[Anum_pg_seqam_seqamsetval - 1])))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("sequence access method setval function is required")));

	if (!OidIsValid(DatumGetObjectId(values[Anum_pg_seqam_seqamgetstate - 1])))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("sequence access method getstate function is required")));

	if (!OidIsValid(DatumGetObjectId(values[Anum_pg_seqam_seqamsetstate - 1])))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("sequence access method setstate function is required")));

	/*
	 * Insert tuple into pg_seqam.
	 */
	rel = heap_open(SeqAccessMethodRelationId, RowExclusiveLock);

	tuple = heap_form_tuple(rel->rd_att, values, nulls);

	seqamoid = simple_heap_insert(rel, tuple);

	CatalogUpdateIndexes(rel, tuple);

	makeSeqAMDependencies(tuple);

	heap_freetuple(tuple);

	/* Post creation hook */
	InvokeObjectPostCreateHook(SeqAccessMethodRelationId, seqamoid, 0);

	heap_close(rel, RowExclusiveLock);

	return seqamoid;
}

/*
 * Drop a sequence access method.
 */
void
RemoveSeqAMById(Oid seqamoid)
{
	Relation	rel;
	HeapTuple	tuple;
	Form_pg_seqam seqam;

	/*
	 * Find the target tuple
	 */
	rel = heap_open(SeqAccessMethodRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(SEQAMOID, ObjectIdGetDatum(seqamoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for sequence access method %u",
			 seqamoid);

	seqam = (Form_pg_seqam) GETSTRUCT(tuple);
	/* Can't drop builtin local sequence access method. */
	if (seqamoid == LOCAL_SEQAM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence access method %s",
						NameStr(seqam->seqamname))));

	/*
	 * Remove the pg_seqam tuple (this will roll back if we fail below)
	 */
	simple_heap_delete(rel, &tuple->t_self);

	ReleaseSysCache(tuple);

	heap_close(rel, RowExclusiveLock);
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
