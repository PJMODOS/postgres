/* src/test/modules/tablesample/tsm_test--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION tsm_test" to load this file. \quit

CREATE FUNCTION tsm_test_init(internal, int4, text)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION tsm_test_nextblock(internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION tsm_test_nexttuple(internal, int4, int2)
RETURNS int2
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION tsm_test_examinetuple(internal, int4, internal, bool)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION tsm_test_end(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION tsm_test_reset(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION tsm_test_cost(internal, internal, internal, internal, internal, internal, internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;


CREATE TABLESAMPLE METHOD tsm_test (
	SEQSCAN		= true,
	PAGEMODE	= true,
	INIT		= tsm_test_init,
	NEXTBLOCK	= tsm_test_nextblock,
	NEXTTUPLE	= tsm_test_nexttuple,
	EXAMINETUPLE	= tsm_test_examinetuple,
	END			= tsm_test_end,
	RESET		= tsm_test_reset,
	COST		= tsm_test_cost
);
