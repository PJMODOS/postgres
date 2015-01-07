/*-------------------------------------------------------------------------
 *
 * tablesample.c
 *	  Commands to manipulate tablesample methods
 *
 * Table sampling methods provide algorithms for doing sample scan over
 * the table.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/tablesample.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_tablesample_method.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


static Datum
get_tablesample_method_func(DefElem *defel, int attnum)
{
	List	   *funcName = defGetQualifiedName(defel);
	/* Big enough size for our needs. */
	Oid		   *typeId = palloc0(7 * sizeof(Oid));
	Oid			retTypeId;
	int			nargs;
	Oid			procOid = InvalidOid;
	FuncCandidateList clist;

	switch (attnum)
	{
		case Anum_pg_tablesample_method_tsminit:
			/*
			 * tsminit needs special handling because it is defined as function
			 * with 3 or more arguments and only first two arguments must have
			 * specific type, the rest is up to the tablesample method creator.
			 */
			{
				nargs = 2;
				typeId[0] = INTERNALOID;
				typeId[1] = INT4OID;
				retTypeId = VOIDOID;

				clist = FuncnameGetCandidates(funcName, -1, NIL, false, false, false);

				while (clist)
				{
					if (clist->nargs >= 3 &&
						memcmp(typeId, clist->args, nargs * sizeof(Oid)) == 0)
					{
						procOid = clist->oid;
						/* Save real function signature for future errors. */
						nargs = clist->nargs;
						pfree(typeId);
						typeId = clist->args;
						break;
					}
					clist = clist->next;
				}

				if (!OidIsValid(procOid))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_FUNCTION),
							 errmsg("function \"%s\" does not exist or does not have valid signature",
									NameListToString(funcName)),
							 errhint("The tamplesample method init function "
									 "must have at least 3 input parameters "
									 "with first one of type INTERNAL and second of type INTEGER.")));
			}
			break;

		case Anum_pg_tablesample_method_tsmnextblock:
			nargs = 1;
			typeId[0] = INTERNALOID;
			retTypeId = INT4OID;
			break;
		case Anum_pg_tablesample_method_tsmnexttuple:
			nargs = 3;
			typeId[0] = INTERNALOID;
			typeId[1] = INT4OID;
			typeId[2] = INT2OID;
			retTypeId = INT2OID;
			break;
		case Anum_pg_tablesample_method_tsmexaminetuple:
			nargs = 4;
			typeId[0] = INTERNALOID;
			typeId[1] = INT4OID;
			typeId[2] = INTERNALOID;
			typeId[3] = BOOLOID;
			retTypeId = BOOLOID;
			break;
		case Anum_pg_tablesample_method_tsmend:
		case Anum_pg_tablesample_method_tsmreset:
			nargs = 1;
			typeId[0] = INTERNALOID;
			retTypeId = VOIDOID;
			break;
		case Anum_pg_tablesample_method_tsmcost:
			nargs = 7;
			typeId[0] = INTERNALOID;
			typeId[1] = INTERNALOID;
			typeId[2] = INTERNALOID;
			typeId[3] = INTERNALOID;
			typeId[4] = INTERNALOID;
			typeId[5] = INTERNALOID;
			typeId[6] = INTERNALOID;
			retTypeId = VOIDOID;
			break;
		default:
			/* should not be here */
			elog(ERROR, "unrecognized attribute for tablesample method: %d",
				 attnum);
			nargs = 0;			/* keep compiler quiet */
	}

	if (!OidIsValid(procOid))
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
 * make pg_depend entries for a new pg_tablesample_method entry
 */
static void
makeTablesampleMethodDeps(HeapTuple tuple)
{
	Form_pg_tablesample_method tsm = (Form_pg_tablesample_method) GETSTRUCT(tuple);
	ObjectAddress	myself,
					referenced;

	myself.classId = TableSampleMethodRelationId;
	myself.objectId = HeapTupleGetOid(tuple);
	myself.objectSubId = 0;

	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, false);

	/* dependencies on functions */
	referenced.classId = ProcedureRelationId;
	referenced.objectSubId = 0;

	referenced.objectId = tsm->tsminit;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	referenced.objectId = tsm->tsmnextblock;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	referenced.objectId = tsm->tsmnexttuple;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	if (OidIsValid(tsm->tsmexaminetuple))
	{
		referenced.objectId = tsm->tsmexaminetuple;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}

	referenced.objectId = tsm->tsmend;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	referenced.objectId = tsm->tsmreset;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	referenced.objectId = tsm->tsmcost;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
}

/*
 * Create a table sampling method
 *
 * Only superusers can create a table sampling methods.
 */
ObjectAddress
DefineTablesampleMethod(List *names, List *parameters)
{
	char	   *tsmname = strVal(linitial(names));
	Oid			tsmoid;
	ListCell   *pl;
	Relation	rel;
	Datum		values[Natts_pg_tablesample_method];
	bool		nulls[Natts_pg_tablesample_method];
	HeapTuple	tuple;
	ObjectAddress address;

	/* Must be super user. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create tablesample method \"%s\"",
						tsmname),
				 errhint("Must be superuser to create a tablesample method.")));

	/* Must not already exist. */
	tsmoid = get_tablesample_method_oid(tsmname, true);
	if (OidIsValid(tsmoid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablesample method \"%s\" already exists",
						tsmname)));

	/* Initialize the values. */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_tablesample_method_tsmname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(tsmname));

	/*
	 * loop over the definition list and extract the information we need.
	 */
	foreach(pl, parameters)
	{
		DefElem    *defel = (DefElem *) lfirst(pl);

		if (pg_strcasecmp(defel->defname, "seqscan") == 0)
		{
			values[Anum_pg_tablesample_method_tsmseqscan - 1] =
				BoolGetDatum(defGetBoolean(defel));
		}
		else if (pg_strcasecmp(defel->defname, "pagemode") == 0)
		{
			values[Anum_pg_tablesample_method_tsmpagemode - 1] =
				BoolGetDatum(defGetBoolean(defel));
		}
		else if (pg_strcasecmp(defel->defname, "init") == 0)
		{
			values[Anum_pg_tablesample_method_tsminit - 1] =
				get_tablesample_method_func(defel,
					Anum_pg_tablesample_method_tsminit);
		}
		else if (pg_strcasecmp(defel->defname, "nextblock") == 0)
		{
			values[Anum_pg_tablesample_method_tsmnextblock - 1] =
				get_tablesample_method_func(defel,
					Anum_pg_tablesample_method_tsmnextblock);
		}
		else if (pg_strcasecmp(defel->defname, "nexttuple") == 0)
		{
			values[Anum_pg_tablesample_method_tsmnexttuple - 1] =
				get_tablesample_method_func(defel,
					Anum_pg_tablesample_method_tsmnexttuple);
		}
		else if (pg_strcasecmp(defel->defname, "examinetuple") == 0)
		{
			values[Anum_pg_tablesample_method_tsmexaminetuple - 1] =
				get_tablesample_method_func(defel,
					Anum_pg_tablesample_method_tsmexaminetuple);
		}
		else if (pg_strcasecmp(defel->defname, "end") == 0)
		{
			values[Anum_pg_tablesample_method_tsmend - 1] =
				get_tablesample_method_func(defel,
					Anum_pg_tablesample_method_tsmend);
		}
		else if (pg_strcasecmp(defel->defname, "reset") == 0)
		{
			values[Anum_pg_tablesample_method_tsmreset - 1] =
				get_tablesample_method_func(defel,
					Anum_pg_tablesample_method_tsmreset);
		}
		else if (pg_strcasecmp(defel->defname, "cost") == 0)
		{
			values[Anum_pg_tablesample_method_tsmcost - 1] =
				get_tablesample_method_func(defel,
					Anum_pg_tablesample_method_tsmcost);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("tablesample method parameter \"%s\" not recognized",
						defel->defname)));
	}

	/*
	 * Validation.
	 */
	if (!OidIsValid(DatumGetObjectId(values[Anum_pg_tablesample_method_tsminit - 1])))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("tablesample method init function is required")));

	if (!OidIsValid(DatumGetObjectId(values[Anum_pg_tablesample_method_tsmnextblock - 1])))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("tablesample method nextblock function is required")));

	if (!OidIsValid(DatumGetObjectId(values[Anum_pg_tablesample_method_tsmnexttuple - 1])))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("tablesample method nexttuple function is required")));

	if (!OidIsValid(DatumGetObjectId(values[Anum_pg_tablesample_method_tsmend - 1])))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("tablesample method end function is required")));

	if (!OidIsValid(DatumGetObjectId(values[Anum_pg_tablesample_method_tsmreset - 1])))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("tablesample method reset function is required")));

	if (!OidIsValid(DatumGetObjectId(values[Anum_pg_tablesample_method_tsmcost - 1])))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("tablesample method cost function is required")));

	/*
	 * Insert tuple into pg_tablesample_method.
	 */
	rel = heap_open(TableSampleMethodRelationId, RowExclusiveLock);

	tuple = heap_form_tuple(rel->rd_att, values, nulls);

	tsmoid = simple_heap_insert(rel, tuple);

	CatalogUpdateIndexes(rel, tuple);

	makeTablesampleMethodDeps(tuple);

	heap_freetuple(tuple);

	/* Post creation hook for new tablesample method */
	InvokeObjectPostCreateHook(TableSampleMethodRelationId, tsmoid, 0);

	ObjectAddressSet(address, TableSampleMethodRelationId, tsmoid);

	heap_close(rel, RowExclusiveLock);

	return address;
}

/*
 * Drop a tablesample method.
 */
void
RemoveTablesampleMethodById(Oid tsmoid)
{
	Relation	rel;
	HeapTuple	tuple;
	Form_pg_tablesample_method tsm;

	/*
	 * Find the target tuple
	 */
	rel = heap_open(TableSampleMethodRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(TABLESAMPLEMETHODOID, ObjectIdGetDatum(tsmoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for tablesample method %u",
			 tsmoid);

	tsm = (Form_pg_tablesample_method) GETSTRUCT(tuple);
	/* Can't drop builtin tablesample methods. */
	if (tsmoid == TABLESAMPLE_METHOD_SYSTEM_OID ||
		tsmoid == TABLESAMPLE_METHOD_BERNOULLI_OID)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for tablesample method %s",
						NameStr(tsm->tsmname))));

	/*
	 * Remove the pg_tablespace tuple (this will roll back if we fail below)
	 */
	simple_heap_delete(rel, &tuple->t_self);

	ReleaseSysCache(tuple);

	heap_close(rel, RowExclusiveLock);
}
