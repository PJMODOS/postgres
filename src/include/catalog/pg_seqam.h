/*-------------------------------------------------------------------------
 *
 * pg_seqam.h
 *	  definition of the system "sequence access method" relation (pg_seqam)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_seqam.h
 *
 * NOTES
 *		the genbki.pl script reads this file and generates .bki
 *		information from the DATA() statements.
 *
 *		XXX do NOT break up DATA() statements into multiple lines!
 *			the scripts are not as smart as you might think...
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SEQAM_H
#define PG_SEQAM_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_seqam definition.  cpp turns this into
 *		typedef struct FormData_pg_seqam
 * ----------------
 */
#define SeqAccessMethodRelationId	3286

CATALOG(pg_seqam,3286)
{
	NameData	seqamname;			/* access method name */
	regproc		seqamreloptions;	/* parse AM-specific options */
	regproc		seqaminit;			/* sequence initialization */
	regproc		seqamalloc;			/* get next allocation of range of values function */
	regproc		seqamsetval;		/* set value function */
	regproc		seqamgetstate;		/* dump state, used by pg_dump */
	regproc		seqamsetstate;		/* restore state, used when loading pg_dump */
} FormData_pg_seqam;

/* ----------------
 *		Form_pg_seqam corresponds to a pointer to a tuple with
 *		the format of pg_seqam relation.
 * ----------------
 */
typedef FormData_pg_seqam *Form_pg_seqam;

/* ----------------
 *		compiler constants for pg_seqam
 * ----------------
 */
#define Natts_pg_seqam						7
#define Anum_pg_seqam_seqamname				1
#define Anum_pg_seqam_seqamreloptions		2
#define Anum_pg_seqam_seqaminit				3
#define Anum_pg_seqam_seqamalloc			4
#define Anum_pg_seqam_seqamsetval			5
#define Anum_pg_seqam_seqamgetstate			6
#define Anum_pg_seqam_seqamsetstate			7

/* ----------------
 *		initial contents of pg_seqam
 * ----------------
 */

DATA(insert OID = 6022 (  local		seqam_local_reloptions seqam_local_init seqam_local_alloc seqam_local_setval seqam_local_get_state seqam_local_set_state));
DESCR("local sequence access method");
#define LOCAL_SEQAM_OID 6022

#endif   /* PG_SEQAM_H */
