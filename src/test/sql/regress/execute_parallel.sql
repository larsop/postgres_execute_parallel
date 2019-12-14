CREATE EXTENSION dblink; -- needed by  execute_parallel
select 1, execute_parallel('{"select pg_sleep(10)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)"}'::text[],3);
select 2, execute_parallel('{"select aaaapg_sleep(10)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)"}'::text[],3);
select 3, execute_parallel('{"create table test(c1 int)"}'::text[],3);
select 4, execute_parallel('{"insert  into testfeil) values (2)","insert  into test(c1) values (1)","insert  into test(c1) values (1)","insert  into test(c1) values (1)","insert  into test(c1) values (1)","insert  into test(c1) values (1)","insert  into test(c1) values (1)"}'::text[],3);
select 5, count(*) from test where c1=1;
select 5, count(*) from test where c1=2;
select 6, execute_parallel('{"insert  into test(c1) values (1)","insert  into test(c1) values (1)","insert  into test(c1) values (1)","insert  into test(c1) values (1)","insert  into test(c1) values (1)","insert  into test(c1) values (1)"}'::text[],10);
select 7, count(*) from test where c1=1;
select 8, count(*) from test where c1=2;
drop table test;
