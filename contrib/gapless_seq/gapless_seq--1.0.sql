-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gapless_seq" to load this file. \quit

CREATE OR REPLACE FUNCTION seqam_gapless_reloptions(INTERNAL, BOOLEAN)
RETURNS BYTEA
LANGUAGE C
STABLE STRICT
AS 'MODULE_PATHNAME'
;

CREATE OR REPLACE FUNCTION seqam_gapless_init(INTERNAL, INTERNAL, BIGINT, BOOLEAN, BOOLEAN)
RETURNS BYTEA
LANGUAGE C
STABLE STRICT
AS 'MODULE_PATHNAME'
;

CREATE OR REPLACE FUNCTION seqam_gapless_alloc(INTERNAL, INTERNAL, BIGINT, INTERNAL)
RETURNS BIGINT
LANGUAGE C
STABLE STRICT
AS 'MODULE_PATHNAME'
;

CREATE OR REPLACE FUNCTION seqam_gapless_setval(INTERNAL, INTERNAL, BIGINT)
RETURNS VOID
LANGUAGE C
STABLE STRICT
AS 'MODULE_PATHNAME'
;

CREATE OR REPLACE FUNCTION seqam_gapless_get_state(INTERNAL, INTERNAL)
RETURNS TEXT[]
LANGUAGE C
STABLE STRICT
AS 'MODULE_PATHNAME'
;

CREATE OR REPLACE FUNCTION seqam_gapless_set_state(INTERNAL, INTERNAL, TEXT[])
RETURNS VOID
LANGUAGE C
STABLE STRICT
AS 'MODULE_PATHNAME'
;

CREATE TABLE seqam_gapless_values (
	seqid oid PRIMARY KEY,
	is_called bool NOT NULL,
	last_value bigint NOT NULL
);

CREATE OR REPLACE FUNCTION gapless_seq_clean_sequence_value()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
BEGIN
	-- just delete all the value data that don't have corresponding
	-- gapless sequence (either DELETEd or ALTERed to different AM)
	DELETE FROM gapless_seq.seqam_gapless_values WHERE seqid NOT IN (
		SELECT oid FROM pg_class WHERE relam = (
			SELECT oid FROM pg_seqam WHERE seqamname = 'gapless'
		)
	);
END;
$$;

CREATE EVENT TRIGGER gapless_seq_clean_sequence_value ON sql_drop
	WHEN TAG IN ('DROP SEQUENCE')
	EXECUTE PROCEDURE gapless_seq_clean_sequence_value();

CREATE ACCESS METHOD FOR SEQUENCES gapless AS (
	reloptions = seqam_gapless_reloptions,	/* reloptions parser is same as local (no reloptions) */
	init = seqam_gapless_init,				/* init new gapless sequence */
	alloc = seqam_gapless_alloc,			/* logs and returns each value... slow */
	setval = seqam_gapless_setval,			/* setval support */
	getstate = seqam_gapless_get_state,		/* pgdump support */
	setstate = seqam_gapless_set_state		/* pgdump support */
);
