/* src/test/modules/tablesample/tsm_system_rows--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION tsm_system_rows" to load this file. \quit

CREATE FUNCTION tsm_system_rows_init(internal, int4, int4)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION tsm_system_rows_getnext(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION tsm_system_rows_end(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION tsm_system_rows_reset(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION tsm_system_rows_cost(internal, internal, internal, internal, internal, internal, internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

INSERT INTO pg_tablesample_method VALUES('system_rows', false, true,
	'tsm_system_rows_init', 'tsm_system_rows_getnext',
	'tsm_system_rows_end', 'tsm_system_rows_reset', 'tsm_system_rows_cost');

