/**
 * From Joe Conway <mail@joeconway.com>
 * https://www.postgresql-archive.org/How-to-run-in-parallel-in-Postgres-td6114510.html
 * 
 */

DROP FUNCTION IF EXISTS execute_parallel(stmts text[]);
DROP FUNCTION IF EXISTS execute_parallel(stmts text[], num_parallel_thread int);

CREATE OR REPLACE FUNCTION execute_parallel(stmts text[], num_parallel_thread int DEFAULT 3)
RETURNS int AS
$$
declare
  i int = 1;
  current_stmt_index int = 0;
  num_stmts_executed int = 1;
  num_conn_opened int = 0;
  retv text;
  conn_status int;
  conn text;
  connstr text;
  rv int;
  new_stmts_started boolean; 
  all_stmts_done boolean; 

  db text := current_database();
begin
	
	-- Check if num parallel theads if bugger than num stmts
	IF (num_parallel_thread > array_length(stmts,1)) THEN
  	  	num_parallel_thread = array_length(stmts,1);
  	END IF;

  	
  	-- Open connections for num_parallel_thread
	-- and send off the first batch of jobs
	BEGIN
	  	for i in 1..num_parallel_thread loop
		    conn := 'conn' || i::text;
		    connstr := 'dbname=' || db;
		    perform dblink_connect(conn, connstr);
		    rv := dblink_send_query(conn, stmts[i]);
		    num_conn_opened = num_conn_opened + 1;
		    current_stmt_index = current_stmt_index + 1;
		end loop;
	EXCEPTION WHEN OTHERS THEN
	  	
	  	RAISE NOTICE 'Failed to open all requested onnections % , reduce to  %', num_parallel_thread, num_conn_opened;
	  	
		-- Check if num parallel theads if bugger than num stmts
		IF (num_conn_opened < num_parallel_thread) THEN
	  	  	num_parallel_thread = num_conn_opened;
	  	END IF;

	END;


	IF (num_conn_opened > 0) THEN
	  	-- Enter main loop
	  	LOOP 
	  	  new_stmts_started = false;
	  	  all_stmts_done = true;

		  for i in 1..num_parallel_thread loop
			conn := 'conn' || i::text;
		    select dblink_is_busy(conn) into conn_status;

		    if (conn_status = 0) THEN
			    select val into retv from dblink_get_result(conn) as d(val text);
			    select val into retv from dblink_get_result(conn) as d(val text);
			    IF (current_stmt_index < array_length(stmts,1)) THEN
				    rv := dblink_send_query(conn, stmts[current_stmt_index]);
					current_stmt_index = current_stmt_index + 1;
					all_stmts_done = false;
				END IF;
				new_stmts_started = true;
			ELSE
				all_stmts_done = false;
		    END IF;
		  end loop;
		  RAISE NOTICE 'current_stmt_index =% , array_length= %', current_stmt_index, array_length(stmts,1);
		  EXIT WHEN current_stmt_index = array_length(stmts,1) AND all_stmts_done = true; 
		  
		  -- Do a slepp if nothings happens to reduce CPU load 
		  IF (new_stmts_started = false) THEN 
		  	RAISE NOTICE 'sleep at current_stmt_index =% , array_length= %', current_stmt_index, array_length(stmts,1);
		  	perform pg_sleep(1);
		  END IF;
		END LOOP ;
	
		-- cose connections for num_parallel_thread
	  	for i in 1..num_parallel_thread loop
		    conn := 'conn' || i::text;
		    perform dblink_disconnect(conn);
		end loop;
  END IF;


  return current_stmt_index;
  end;
$$ language plpgsql;

GRANT EXECUTE on FUNCTION execute_parallel(stmts text[], num_parallel_thread int) TO public;

\timing 

 DO $$
   declare
     stmts text[];
     i int;
   begin
 stmts[1] = 'select pg_sleep(10)';
 for i in 2..11 loop
     stmts[i] = 'select pg_sleep(1)';
end loop;
   PERFORM execute_parallel(stmts,2);
   end;
$$ LANGUAGE plpgsql;
