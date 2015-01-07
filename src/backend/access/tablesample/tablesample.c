/*-------------------------------------------------------------------------
 *
 * tablesample.c
 *        TABLESAMPLE internal api
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *        src/backend/access/tablesample/tablesample.c
 *
 * TABLESAMPLE is SQL standard clause for sampling the relations.
 *
 * The API provided here provides interface between executor and the
 * TABLESAMPLE Methods.
 *
 * TABLESAMPLE Methods are implementations of actual sampling algorithms which
 * can be used for returning sample of the source relation.
 * Methods don't read the table directly but are asked for block numeber and
 * tuple offset which they want to examine (or return) and the tablesample
 * interface implemented here does the reading for them.
 *
 * We currently only support sampling of the physical relations, but in the
 * future we might exten the API to support subqueries as well.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/tablesample.h"

#include "catalog/pg_tablesample_method.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "utils/rel.h"
#include "utils/tqual.h"


static bool SampleTupleVisible(HeapTuple tuple, OffsetNumber tupoffset, HeapScanDesc scan);


/*
 * Initialize the TABLESAMPLE Descriptor and the TABLESAMPLE Method.
 */
TableSampleDesc *
tablesample_init(SampleScanState *scanstate, TableSampleClause *tablesample)
{
	FunctionCallInfoData fcinfo;
	int			i;
	List	   *args = tablesample->args;
	ListCell   *arg;
	ExprContext *econtext = scanstate->ss.ps.ps_ExprContext;
	TableSampleDesc	*tsdesc = (TableSampleDesc *) palloc0(sizeof(TableSampleDesc));

	/* Load functions */
	fmgr_info(tablesample->tsminit, &(tsdesc->tsminit));
	fmgr_info(tablesample->tsmgetnext, &(tsdesc->tsmgetnext));
	fmgr_info(tablesample->tsmreset, &(tsdesc->tsmreset));
	fmgr_info(tablesample->tsmend, &(tsdesc->tsmend));

	InitFunctionCallInfoData(fcinfo, &tsdesc->tsminit,
							 list_length(args) + 2,
							 InvalidOid, NULL, NULL);

	tsdesc->tupDesc = scanstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	tsdesc->heapScan = scanstate->ss.ss_currentScanDesc;

	/* First argument for init function is always TableSampleDesc */
	fcinfo.arg[0] = PointerGetDatum(tsdesc);
	fcinfo.argnull[0] = false;

	/*
	 * Second arg for init function is always REPEATABLE
	 * When tablesample->repeatable is NULL then REPEATABLE clause was not
	 * specified.
	 * When specified, the expression cannot evaluate to NULL.
	 */
	if (tablesample->repeatable)
	{
		ExprState  *argstate = ExecInitExpr((Expr *) tablesample->repeatable,
											(PlanState *) scanstate);
		fcinfo.arg[1] = ExecEvalExpr(argstate, econtext,
									 &fcinfo.argnull[1], NULL);
		if (fcinfo.argnull[1])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("REPEATABLE clause must be NOT NULL numeric value")));
	}
	else
	{
		fcinfo.arg[1] = UInt32GetDatum(random());
		fcinfo.argnull[1] = false;
	}

	/* Rest of the arguments come from user.  */
	i = 2;
	foreach(arg, args)
	{
		Expr	   *argexpr = (Expr *) lfirst(arg);
		ExprState  *argstate = ExecInitExpr(argexpr, (PlanState *) scanstate);

		if (argstate == NULL)
		{
			fcinfo.argnull[i] = true;
			fcinfo.arg[i] = (Datum) 0;;
		}

		fcinfo.arg[i] = ExecEvalExpr(argstate, econtext,
									 &fcinfo.argnull[i], NULL);
		i++;
	}
	Assert(i == fcinfo.nargs);

	(void) FunctionCallInvoke(&fcinfo);

	return tsdesc;
}

/*
 * Get next tuple from TABLESAMPLE Method.
 */
HeapTuple
tablesample_getnext(TableSampleDesc *desc)
{
	FunctionCallInfoData fcinfo;

	fcinfo.arg[0] = PointerGetDatum(desc);
	fcinfo.argnull[0] = false;

	InitFunctionCallInfoData(fcinfo, &desc->tsmgetnext,
							 1, InvalidOid, NULL, NULL);

	return (HeapTuple) DatumGetPointer(FunctionCallInvoke(&fcinfo));
}

/*
 * Get next tuple from the source input.
 *
 * Called by the TABLESAMPLE Method.
 */
HeapTuple
tablesample_source_getnext(TableSampleDesc *desc)
{
	return heap_getnext(desc->heapScan, ForwardScanDirection);
}

/*
 * Get specific tuple as specified by TID from the source input.
 *
 * Called by the TABLESAMPLE Method.
 */
HeapTuple
tablesample_source_gettup(TableSampleDesc *desc, ItemPointer tid,
						  bool *visible)
{
	HeapScanDesc	scan = desc->heapScan;
	HeapTuple		tuple = &(scan->rs_ctup);
	BlockNumber		blockno = ItemPointerGetBlockNumber(tid);
	OffsetNumber	tupoffset = ItemPointerGetOffsetNumber(tid);
	Page			page;
	ItemId			itemid;
	bool			pagemode;

	Assert(scan->rs_inited);

	if (!BlockNumberIsValid(blockno) || !OffsetNumberIsValid(tupoffset))
	{
		tuple->t_data = NULL;
		return NULL;
	}

	pagemode = scan->rs_pageatatime;

	/* different page than last call */
	if (blockno != scan->rs_cblock)
		heapgetpage(scan, blockno);

	/*
	 * When pagemode is disabled, the scan will do visibility checks for each
	 * tuple it finds so the buffer needs to be locked.
	 */
	if (!pagemode)
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

	page = (Page) BufferGetPage(scan->rs_cbuf);

	/* Skip invalid tuple pointers. */
	itemid = PageGetItemId(page, tupoffset);
	if (!ItemIdIsNormal(itemid))
			return NULL;

	tuple->t_data = (HeapTupleHeader) PageGetItem((Page) page, itemid);
	tuple->t_len = ItemIdGetLength(itemid);
	ItemPointerSet(&(tuple->t_self), blockno, tupoffset);

	*visible = PageIsAllVisible(page) ||
		SampleTupleVisible(tuple, tupoffset, scan);

	if (!pagemode)
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

	pgstat_count_heap_getnext(scan->rs_rd);

	return tuple;
}

/*
 * Reset the sampling to starting state
 */
void
tablesample_reset(TableSampleDesc *desc)
{
	(void) FunctionCall1(&desc->tsmreset, PointerGetDatum(desc));
}

/*
 * Signal the sampling method that the scan has finished.
 */
void
tablesample_end(TableSampleDesc *desc)
{
	(void) FunctionCall1(&desc->tsmend, PointerGetDatum(desc));
}

/*
 * Check visibility of the tuple.
 */
static bool
SampleTupleVisible(HeapTuple tuple, OffsetNumber tupoffset, HeapScanDesc scan)
{
	/*
	 * If this scan is reading whole pages at a time, there is already
	 * visibility info present in rs_vistuples so we can just search it
	 * for the tupoffset.
	 */
	if (scan->rs_pageatatime)
	{
		int start = 0,
			end = scan->rs_ntuples - 1;

		/*
		 * Do the binary search over rs_vistuples, it's already sorted by
		 * OffsetNumber so we don't need to do any sorting ourselves here.
		 *
		 * We could use bsearch() here but it's slower for integers because
		 * of the function call overhead and because it needs boiler plate code
		 * it would not save us anything code-wise anyway.
		 */
		while (start <= end)
		{
			int mid = start + (end - start) / 2;
			OffsetNumber curoffset = scan->rs_vistuples[mid];

			if (curoffset == tupoffset)
				return true;
			else if (curoffset > tupoffset)
				end = mid - 1;
			else
				start = mid + 1;
		}

		return false;
	}
	else
	{
		/* No pagemode, we have to check the tuple itself. */
		Snapshot	snapshot = scan->rs_snapshot;
		Buffer		buffer = scan->rs_cbuf;

		bool visible = HeapTupleSatisfiesVisibility(tuple, snapshot, buffer);

		CheckForSerializableConflictOut(visible, scan->rs_rd, tuple, buffer,
										snapshot);

		return visible;
	}
}
