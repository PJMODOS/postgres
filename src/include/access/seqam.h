/*-------------------------------------------------------------------------
 *
 * seqam.h
 *	  Public header file for Sequence access method.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/seqam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SEQAM_H
#define SEQAM_H

#include "fmgr.h"

#include "access/htup.h"
#include "commands/sequence.h"
#include "utils/relcache.h"
#include "storage/buf.h"
#include "storage/bufpage.h"


struct SequenceHandle;
typedef struct SequenceHandle SequenceHandle;

extern char *serial_seqam;

extern Oid DefineSeqAM(List *names, List *definition);
extern void RemoveSeqAMById(Oid seqamoid);

extern HeapTuple seqam_init(Relation seqrel, HeapTuple tuple,
							int64 restart_value, bool restart_requested,
							bool is_init);
extern int64 seqam_alloc(Relation seqrel, SequenceHandle *seqh,
						 int64 nrequested, int64 *last);
extern void seqam_setval(Relation seqrel, SequenceHandle *seqh,
						 int64 new_value);
extern ArrayType * seqam_get_state(Relation seqrel, SequenceHandle *seqh);
extern void seqam_set_state(Relation seqrel, SequenceHandle *seqh,
					 ArrayType *statearr);

extern Oid get_seqam_oid(const char *sequencename, bool missing_ok);

extern void sequence_open(Oid seqrelid, SequenceHandle *seqh);
extern void sequence_close(SequenceHandle *seqh);
extern HeapTuple sequence_read_tuple(SequenceHandle *seqh);
extern void sequence_swap_tuple(SequenceHandle *seqh, HeapTuple newtup);
extern void sequence_start_update(SequenceHandle *seqh, bool do_wal);
extern void sequence_apply_update(SequenceHandle *seqh, bool do_wal);
extern void sequence_finish_update(SequenceHandle *seqh);
extern void sequence_release_tuple(SequenceHandle *seqh);
extern bool sequence_needs_wal(SequenceHandle *seqh);

extern int64 sequence_increment(Relation seqrel, int64 *value, int64 incnum,
								int64 minv, int64 maxv, int64 incby,
								bool is_cycled, bool report_errors);
extern void sequence_check_range(int64 value, int64 min_value,
								 int64 max_value, const char *valname);
extern int64 sequence_get_restart_value(List *options, int64 default_value,
										bool *found);

extern Datum seqam_local_reloptions(PG_FUNCTION_ARGS);
extern Datum seqam_local_init(PG_FUNCTION_ARGS);
extern Datum seqam_local_alloc(PG_FUNCTION_ARGS);
extern Datum seqam_local_setval(PG_FUNCTION_ARGS);
extern Datum seqam_local_get_state(PG_FUNCTION_ARGS);
extern Datum seqam_local_set_state(PG_FUNCTION_ARGS);

#endif   /* SEQAM_H */
