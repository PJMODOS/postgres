CREATE EXTENSION tsm_test;

CREATE TABLE test_tsm AS SELECT md5(i::text) a, 0.5::float b FROM generate_series(1,10) g(i);

SELECT * FROM test_tsm TABLESAMPLE tsm_test('b') REPEATABLE (1);

CREATE VIEW test_tsm_v AS SELECT * FROM test_tsm TABLESAMPLE tsm_test('b') REPEATABLE (9999);
SELECT * FROM test_tsm_v;

DROP TABLESAMPLE METHOD tsm_test;
DROP EXTENSION tsm_test;
DROP EXTENSION tsm_test CASCADE;

SELECT * FROM pg_tablesample_method WHERE tsmname = 'tsm_test';
