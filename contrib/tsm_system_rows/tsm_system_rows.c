/*-------------------------------------------------------------------------
 *
 * tsm_system_rows.c
 *	  interface routines for system_rows tablesample method
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/tsm_system_rows_rowlimit/tsm_system_rows.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "access/tablesample.h"
#include "access/relscan.h"
#include "nodes/execnodes.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "storage/bufmgr.h"
#include "utils/sampling.h"

PG_MODULE_MAGIC;

/*
 * State
 */
typedef struct
{
	BlockSamplerData bs;
	uint32 seed;				/* random seed */
	BlockNumber nblocks;		/* number of block in relation */
	int32 ntuples;				/* number of tuples to return */
	int32 donetuples;			/* tuples already returned */
	OffsetNumber lt;			/* last tuple returned from current block */
	BlockNumber lb;				/* last block */
} SystemSamplerData;


PG_FUNCTION_INFO_V1(tsm_system_rows_init);
PG_FUNCTION_INFO_V1(tsm_system_rows_getnext);
PG_FUNCTION_INFO_V1(tsm_system_rows_end);
PG_FUNCTION_INFO_V1(tsm_system_rows_reset);
PG_FUNCTION_INFO_V1(tsm_system_rows_cost);


/*
 * Initializes the state.
 */
Datum
tsm_system_rows_init(PG_FUNCTION_ARGS)
{
	TableSampleDesc	   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	uint32				seed = PG_GETARG_UINT32(1);
	int32				ntuples = PG_ARGISNULL(2) ? -1 : PG_GETARG_INT32(2);
	HeapScanDesc		scan = tsdesc->heapScan;
	SystemSamplerData  *sampler;

	if (ntuples < 1)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("invalid sample size"),
				 errhint("Sample size must be psoitive integer value.")));

	sampler = palloc0(sizeof(SystemSamplerData));

	/* Remember initial values for reinit */
	sampler->seed = seed;
	sampler->nblocks = scan->rs_nblocks;
	sampler->ntuples = ntuples;
	sampler->donetuples = 0;
	sampler->lt = InvalidOffsetNumber;

	/*
	 * We don't know how many blocks we'll need but we assume that on average
	 * there is at least one visible tuple per block.
	 */
	BlockSampler_Init(&sampler->bs, sampler->nblocks,
					  Min(sampler->ntuples, sampler->ntuples), sampler->seed);

	tsdesc->tsmdata = (void *) sampler;

	PG_RETURN_VOID();
}

/*
 * Get next tuple.
 */
Datum
tsm_system_rows_getnext(PG_FUNCTION_ARGS)
{
	TableSampleDesc	   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	SystemSamplerData  *sampler = (SystemSamplerData *) tsdesc->tsmdata;
	HeapTuple			tuple;
	ItemPointerData		tid;
	bool				visible = false;
	BlockNumber			blockno = sampler->lb;
	OffsetNumber		tupoffset = sampler->lt;

	if (sampler->donetuples >= sampler->ntuples)
		PG_RETURN_NULL();

	/* Find next visible tuple. */
	while (!visible)
	{
		if (blockno == InvalidBlockNumber || tupoffset == InvalidOffsetNumber)
		{
			if (!BlockSampler_HasMore(&sampler->bs))
				PG_RETURN_NULL();

			blockno = BlockSampler_Next(&sampler->bs);
		}

		if (tupoffset == InvalidOffsetNumber)
			tupoffset = FirstOffsetNumber;
		else
			tupoffset++;

		ItemPointerSet(&tid, blockno, tupoffset);
		tuple = tablesample_source_gettup(tsdesc, &tid, &visible);
		if (!tuple)
			continue;
	}

	sampler->lb = blockno;
	sampler->lt = tupoffset;

	sampler->donetuples++;

	PG_RETURN_POINTER(tuple);
}

/*
 * Cleanup method.
 */
Datum
tsm_system_rows_end(PG_FUNCTION_ARGS)
{
	TableSampleDesc *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);

	pfree(tsdesc->tsmdata);

	PG_RETURN_VOID();
}

/*
 * Reset state (called by ReScan).
 */
Datum
tsm_system_rows_reset(PG_FUNCTION_ARGS)
{
	TableSampleDesc	   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	SystemSamplerData  *sampler = (SystemSamplerData *) tsdesc->tsmdata;

	sampler->lt = InvalidOffsetNumber;
	sampler->donetuples = 0;
	BlockSampler_Init(&sampler->bs, sampler->nblocks,
					  Min(sampler->ntuples, sampler->ntuples), sampler->seed);

	PG_RETURN_VOID();
}

/*
 * Costing function.
 */
Datum
tsm_system_rows_cost(PG_FUNCTION_ARGS)
{
	PlannerInfo	   *root = (PlannerInfo *) PG_GETARG_POINTER(0);
	Path		   *path = (Path *) PG_GETARG_POINTER(1);
	RelOptInfo	   *baserel = (RelOptInfo *) PG_GETARG_POINTER(2);
	List		   *args = (List *) PG_GETARG_POINTER(3);
	BlockNumber	   *pages = (BlockNumber *) PG_GETARG_POINTER(4);
	double		   *tuples = (double *) PG_GETARG_POINTER(5);
	Node		   *limitnode;
	int32			ntuples;

	limitnode = linitial(args);
	limitnode = estimate_expression_value(root, limitnode);

	if (IsA(limitnode, RelabelType))
		limitnode = (Node *) ((RelabelType *) limitnode)->arg;

	if (IsA(limitnode, Const))
		ntuples = DatumGetInt32(((Const *) limitnode)->constvalue);
	else
	{
		/* Default ntuples if the estimation didn't return Const. */
		ntuples = 1000;
	}

	*pages = Min(baserel->pages, ntuples);
	*tuples = ntuples;
	path->rows = *tuples;

	PG_RETURN_VOID();
}

