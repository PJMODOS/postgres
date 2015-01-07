/*-------------------------------------------------------------------------
 *
 * tsm_test.c
 *	  Simple example of a custom tablesample method
 *
 * Copyright (c) 2007-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/tablesample/tsm_test.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "access/tablesample.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "nodes/execnodes.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/sampling.h"

PG_MODULE_MAGIC;

/* State */
typedef struct
{
	uint32		seed;			/* random seed */
	AttrNumber	attnum;			/* column to check */
	BlockNumber startblock;		/* starting block, we use ths for syncscan support */
	BlockNumber nblocks;		/* total blocks in relation */
	BlockNumber blockno;		/* current block */
	OffsetNumber lt;			/* last tuple returned from current block */
	SamplerRandomState randstate; /* random generator state */
} TestSamplerState;


PG_FUNCTION_INFO_V1(tsm_test_init);
PG_FUNCTION_INFO_V1(tsm_test_nextblock);
PG_FUNCTION_INFO_V1(tsm_test_nexttuple);
PG_FUNCTION_INFO_V1(tsm_test_examinetuple);
PG_FUNCTION_INFO_V1(tsm_test_end);
PG_FUNCTION_INFO_V1(tsm_test_reset);
PG_FUNCTION_INFO_V1(tsm_test_cost);

/*
 * Initialize the state.
 */
Datum
tsm_test_init(PG_FUNCTION_ARGS)
{
	TableSampleDesc	   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	uint32				seed = PG_GETARG_UINT32(1);
	char			   *attname;
	AttrNumber			attnum;
	Oid					atttype;
	HeapScanDesc		scan = tsdesc->heapScan;
	TestSamplerState   *sampler;

	if (PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("invalid parameter for tablesample method tsm_test"),
				 errhint("attnum cannot be NULL.")));

	attname = text_to_cstring(PG_GETARG_TEXT_P(2));

	attnum = get_attnum(scan->rs_rd->rd_id, attname);
	if (!AttrNumberIsForUserDefinedAttr(attnum))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid parameter for tablesample method tsm_test"),
				 errhint("column %s does not exist", attname)));

	atttype = get_atttype(scan->rs_rd->rd_id, attnum);
	if (atttype != FLOAT8OID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid parameter for tablesample method tsm_test"),
				 errhint("column %s is not of type float.", attname)));

	sampler = palloc0(sizeof(TestSamplerState));

	/* Remember initial values for reinit */
	sampler->seed = seed;
	sampler->attnum = attnum;
	sampler->startblock = scan->rs_startblock;
	sampler->nblocks = scan->rs_nblocks;
	sampler->blockno = InvalidBlockNumber;
	sampler->lt = InvalidOffsetNumber;
	sampler_random_init_state(sampler->seed, sampler->randstate);

	tsdesc->tsmdata = sampler;

	PG_RETURN_VOID();
}

/*
 * Get next block number to read or InvalidBlockNumber if we are at the
 * end of the relation.
 */
Datum
tsm_test_nextblock(PG_FUNCTION_ARGS)
{
	TableSampleDesc	   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	TestSamplerState   *sampler = (TestSamplerState *) tsdesc->tsmdata;

	/* Cycle from startblock to startblock to support syncscan. */
	if (sampler->blockno == InvalidBlockNumber)
		sampler->blockno = sampler->startblock;
	else
	{
		sampler->blockno++;

		if (sampler->blockno >= sampler->nblocks)
			sampler->blockno = 0;

		if (sampler->blockno == sampler->startblock)
			PG_RETURN_UINT32(InvalidBlockNumber);
	}

	PG_RETURN_UINT32(sampler->blockno);
}

/*
 * Get next tuple from current block.
 */
Datum
tsm_test_nexttuple(PG_FUNCTION_ARGS)
{
	TableSampleDesc	   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	OffsetNumber		maxoffset = PG_GETARG_UINT16(2);
	TestSamplerState   *sampler = (TestSamplerState *) tsdesc->tsmdata;

	if (sampler->lt == InvalidOffsetNumber)
		sampler->lt = FirstOffsetNumber;
	else if (++sampler->lt > maxoffset)
		PG_RETURN_UINT16(InvalidOffsetNumber);

	PG_RETURN_UINT16(sampler->lt);
}

/*
 * Examine tuple and decide if it should be returned.
 */
Datum
tsm_test_examinetuple(PG_FUNCTION_ARGS)
{
	TableSampleDesc	   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	HeapTuple			tuple = (HeapTuple) PG_GETARG_POINTER(2);
	bool				visible = PG_GETARG_BOOL(3);
	TestSamplerState   *sampler = (TestSamplerState *) tsdesc->tsmdata;
	bool				isnull;
	float8				val, rand;

	if (!visible)
		PG_RETURN_BOOL(false);

	val = DatumGetFloat8(heap_getattr(tuple, sampler->attnum, tsdesc->tupDesc, &isnull));
	rand = sampler_random_fract(sampler->randstate);
	if (isnull || val < rand)
		PG_RETURN_BOOL(false);
	else
		PG_RETURN_BOOL(true);
}

/*
 * Cleanup method.
 */
Datum
tsm_test_end(PG_FUNCTION_ARGS)
{
	TableSampleDesc *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);

	pfree(tsdesc->tsmdata);

	PG_RETURN_VOID();
}

/*
 * Reset state (called by ReScan).
 */
Datum
tsm_test_reset(PG_FUNCTION_ARGS)
{
	TableSampleDesc	   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	TestSamplerState   *sampler = (TestSamplerState *) tsdesc->tsmdata;

	sampler->blockno = InvalidBlockNumber;
	sampler->lt = InvalidOffsetNumber;

	sampler_random_init_state(sampler->seed, sampler->randstate);

	PG_RETURN_VOID();
}

/*
 * Costing function.
 */
Datum
tsm_test_cost(PG_FUNCTION_ARGS)
{
	Path		   *path = (Path *) PG_GETARG_POINTER(1);
	RelOptInfo	   *baserel = (RelOptInfo *) PG_GETARG_POINTER(2);
	BlockNumber	   *pages = (BlockNumber *) PG_GETARG_POINTER(4);
	double		   *tuples = (double *) PG_GETARG_POINTER(5);

	*pages = baserel->pages;

	/* This is very bad estimation */
	*tuples = path->rows = path->rows/2;

	PG_RETURN_VOID();
}

