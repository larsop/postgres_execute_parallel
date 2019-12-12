CREATE EXTENSION dblink; -- needed by  execute_parallel
select 1, execute_parallel('{"select pg_sleep(10)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)"}'::text[],3);
