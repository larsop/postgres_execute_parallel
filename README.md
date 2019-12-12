# What is this function doing ?
Postgres sql code to execute an array of statements in parallel based on dblink.


#Returns true if all statements are execute OK.

# How to use :
Her send 11 statements to be executed and execute 3 statements i parallel.

<pre><code>
select 1, execute_parallel('{"select pg_sleep(10)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)","select pg_sleep(1)"}'::text[],3);
</pre></code>

In this case the following is hapning :
- The first statements takes 10 seconds and the first db connection works this all the time this statement.
- The 2 other connections continue to work we the rest of the statements . 

If you request to run more parallel requests than there are db connections available, the number of parallel jobs will be reduced to the number of available connections. 


# How to install :

git clone https://github.com/larsop/postgres_execute_parallel.git

cat postgres_execute_parallel/src/main/sql/function*.sql | psql

psql -c'CREATE EXTENSION dblink;'



