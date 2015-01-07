/*-------------------------------------------------------------------------
 *
 * bernoulli.c
 *	  interface routines for BERNOULLI tablesample method
 *
 * This implementation does the sampling inside the nexttuple interface.
 * Although it could be also done using the examinetuple interface, doing
 * the sampling in nexttuple is faster on physical relations as we don't
 * have to do visibility checks for tuple that are outside of the sample.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/tablesample/bernoulli.c
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


/* tsdesc */
typedef struct
{
	uint32 seed;				/* random seed */
	BlockNumber startblock;		/* starting block, we use ths for syncscan support */
	BlockNumber nblocks;		/* number of blocks */
	float4 probability;			/* probabilty that tuple will be returned (0.0-1.0) */
	SamplerRandomState randstate; /* random generator tsdesc */
} BernoulliSamplerData;

/*
 * Initialize the state.
 */
Datum
tsm_bernoulli_init(PG_FUNCTION_ARGS)
{
	TableSampleDesc	   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	uint32				seed = PG_GETARG_UINT32(1);
	float4				percent = PG_ARGISNULL(2) ? -1 : PG_GETARG_FLOAT4(2);
	HeapScanDesc		scan = tsdesc->heapScan;
	BernoulliSamplerData *sampler;

	if (percent < 0 || percent > 100)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("invalid sample size"),
				 errhint("Sample size must be numeric value between 0 and 100 (inclusive).")));

	sampler = palloc0(sizeof(BernoulliSamplerData));

	/* Remember initial values for reinit */
	sampler->seed = seed;
	sampler->startblock = scan->rs_startblock;
	sampler->nblocks = scan->rs_nblocks;
	sampler->probability = percent / 100;
	sampler_random_init_state(sampler->seed, sampler->randstate);

	tsdesc->tsmdata = (void *) sampler;

	PG_RETURN_VOID();
}

/*
 * Get next tuple.
 *
 * This method implements the main logic in bernoulli sampling.
 * The algorithm simply generates new random number (in 0.0-1.0 range) and if
 * it falls within user specified probability (in the same range) return the
 * tuple.
 */
Datum
tsm_bernoulli_getnext(PG_FUNCTION_ARGS)
{
	TableSampleDesc		   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	BernoulliSamplerData   *sampler =
		(BernoulliSamplerData *) tsdesc->tsmdata;
	HeapTuple				tuple;
	float4					probability = sampler->probability;

	/*
	 * Loop over tuples until the random generator returns value that
	 * is within the probability of returning the tuple or until we reach
	 * end of the table.
	 *
	 * (This is our implementation of bernoulli trial)
	 */
	do
	{
		tuple =	tablesample_source_getnext(tsdesc);
		if (tuple == NULL)
			PG_RETURN_NULL();
	}
	while (sampler_random_fract(sampler->randstate) > probability);

	PG_RETURN_POINTER(tuple);
}

/*
 * Cleanup method.
 */
Datum
tsm_bernoulli_end(PG_FUNCTION_ARGS)
{
	TableSampleDesc *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);

	pfree(tsdesc->tsmdata);

	PG_RETURN_VOID();
}

/*
 * Reset tsdesc (called by ReScan).
 */
Datum
tsm_bernoulli_reset(PG_FUNCTION_ARGS)
{
	TableSampleDesc		   *tsdesc = (TableSampleDesc *) PG_GETARG_POINTER(0);
	BernoulliSamplerData   *sampler =
		(BernoulliSamplerData *) tsdesc->tsmdata;

	sampler_random_init_state(sampler->seed, sampler->randstate);

	PG_RETURN_VOID();
}

/*
 * Costing function.
 */
Datum
tsm_bernoulli_cost(PG_FUNCTION_ARGS)
{
	PlannerInfo	   *root = (PlannerInfo *) PG_GETARG_POINTER(0);
	Path		   *path = (Path *) PG_GETARG_POINTER(1);
	RelOptInfo	   *baserel = (RelOptInfo *) PG_GETARG_POINTER(2);
	List		   *args = (List *) PG_GETARG_POINTER(3);
	BlockNumber	   *pages = (BlockNumber *) PG_GETARG_POINTER(4);
	double		   *tuples = (double *) PG_GETARG_POINTER(5);
	Node		   *pctnode;
	float4			samplesize;

	*pages = baserel->pages;

	pctnode = linitial(args);
	pctnode = estimate_expression_value(root, pctnode);

	if (IsA(pctnode, RelabelType))
		pctnode = (Node *) ((RelabelType *) pctnode)->arg;

	if (IsA(pctnode, Const))
	{
		samplesize = DatumGetFloat4(((Const *) pctnode)->constvalue);
		samplesize /= 100.0;
	}
	else
	{
		/* Default samplesize if the estimation didn't return Const. */
		samplesize = 0.1f;
	}

	*tuples = path->rows * samplesize;
	path->rows = *tuples;

	PG_RETURN_VOID();
}
