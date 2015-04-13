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
} SystemSamplerData;


PG_FUNCTION_INFO_V1(tsm_system_rows_init);
PG_FUNCTION_INFO_V1(tsm_system_rows_nextblock);
PG_FUNCTION_INFO_V1(tsm_system_rows_nexttuple);
PG_FUNCTION_INFO_V1(tsm_system_rows_examinetuple);
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
 * Get next block number or InvalidBlockNumber when we're done.
 *
 * Uses the same logic as VACUUM for picking the random blocks.
 */
Datum
tsm_system_rows_nextblock(PG_FUNCTION_ARGS)
{
	TableSampleDesc	   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	SystemSamplerData  *sampler = (SystemSamplerData *) tsdesc->tsmdata;
	BlockNumber			blockno;

	if (!BlockSampler_HasMore(&sampler->bs) ||
		sampler->donetuples >= sampler->ntuples)
		PG_RETURN_UINT32(InvalidBlockNumber);

	blockno = BlockSampler_Next(&sampler->bs);

	PG_RETURN_UINT32(blockno);
}

/*
 * Get next tuple offset in current block or InvalidOffsetNumber if we are done
 * with this block.
 */
Datum
tsm_system_rows_nexttuple(PG_FUNCTION_ARGS)
{
	TableSampleDesc	   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	OffsetNumber		maxoffset = PG_GETARG_UINT16(2);
	SystemSamplerData  *sampler = (SystemSamplerData *) tsdesc->tsmdata;
	OffsetNumber		tupoffset = sampler->lt;

	if (tupoffset == InvalidOffsetNumber)
		tupoffset = FirstOffsetNumber;
	else
		tupoffset++;

	if (tupoffset > maxoffset ||
		sampler->donetuples >= sampler->ntuples)
		tupoffset = InvalidOffsetNumber;

	sampler->lt = tupoffset;

	PG_RETURN_UINT16(tupoffset);
}

/*
 * Examine tuple and decide if it should be returned.
 */
Datum
tsm_system_rows_examinetuple(PG_FUNCTION_ARGS)
{
	TableSampleDesc	   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	bool				visible = PG_GETARG_BOOL(3);
	SystemSamplerData  *sampler = (SystemSamplerData *) tsdesc->tsmdata;

	if (!visible)
		PG_RETURN_BOOL(false);

	sampler->donetuples++;

	PG_RETURN_BOOL(true);
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

