# What is this function doing ?
Postgres sql code to execute an array of statements in parallel based on dblink.

Returns the number of ok executed statements.

[![Build Status](https://travis-ci.org/larsop/postgres_execute_parallel.svg?branch=master)](https://travis-ci.org/larsop/postgres_execute_parallel)

# How to use :
In the example below we send 11 statements to be executed in three parallel dblink connections .

<pre><code>
select 1, execute_parallel('{"select pg_sleep(10)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)"}'::text[],3);
</pre></code>

In this case the following is hapning :
- The first statements takes 10 seconds and the first dblink connection works this all the time with this statement.
- The 2 other connections continue to work with the rest of the statements while the first dblink connection work the first statement. 

This mean that that this jobs takes a total of 10 seconds if there are three free database connections .

If you request to run more parallel requests than there are db connections available, 
the number of parallel jobs will be reduced to the number of available connections. 

If you have a error in one of the statements an exception will be thrown with the error.

It execute the rest of the statements of _contiune_after_stat_exception is set false when it's callled


# How to install :

git clone https://github.com/larsop/postgres_execute_parallel.git

cat postgres_execute_parallel/src/main/sql/function*.sql | psql

psql -c'CREATE EXTENSION dblink;'

# Info
Based on initial code from Joe Conway <mail@joeconway.com>  in https://www.postgresql-archive.org/How-to-run-in-parallel-in-Postgres-td6114510.html
 

