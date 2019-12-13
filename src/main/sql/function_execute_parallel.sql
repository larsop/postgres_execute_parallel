/**
 * Based on code from Joe Conway <mail@joeconway.com>
 * https://www.postgresql-archive.org/How-to-run-in-parallel-in-Postgres-td6114510.html
 * 
 */

DROP FUNCTION IF EXISTS execute_parallel(stmts text[]);
DROP FUNCTION IF EXISTS execute_parallel(stmts text[], num_parallel_thread int);

-- TODO add test return value
-- TODO catch error on main loop to be sure connenctinos are closed

CREATE OR REPLACE FUNCTION execute_parallel(stmts text[], num_parallel_thread int DEFAULT 3)
RETURNS boolean AS
$$
declare
  i int = 1;
  current_stmt_index int = 1;
  current_stmt_sent int = 0;
  num_stmts_executed int = 1;
  num_stmts_failed int = 0;
  num_conn_opened int = 0;
  retv text;
  retvnull text;
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
		    num_conn_opened = num_conn_opened + 1;
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
		    	BEGIN
				    select val into retv from dblink_get_result(conn) as d(val text);
			  		--RAISE NOTICE 'current_stmt_index =% , val1 status= %', current_stmt_index, retv;
				    -- Two times to reuse connecton according to doc.
				    select val into retvnull from dblink_get_result(conn) as d(val text);
			  		--RAISE NOTICE 'current_stmt_index =% , val2 status= %', current_stmt_index, retv;
				EXCEPTION WHEN OTHERS THEN
					RAISE NOTICE 'Got an error for conn %  retv %', conn, retv;
					num_stmts_failed = num_stmts_failed + 1;
				END;
			    IF (current_stmt_index <= array_length(stmts,1)) THEN
			   		RAISE NOTICE 'Call stmt %  on connection  %', stmts[current_stmt_index], conn;
				    rv := dblink_send_query(conn, stmts[current_stmt_index]);
					current_stmt_index = current_stmt_index + 1;
					all_stmts_done = false;
					new_stmts_started = true;
				END IF;
			ELSE
				all_stmts_done = false;
		    END IF;

		    
		  end loop;
-- 		  RAISE NOTICE 'current_stmt_index =% , array_length= %', current_stmt_index, array_length(stmts,1);
		  EXIT WHEN (current_stmt_index - 1) = array_length(stmts,1) AND all_stmts_done = true AND new_stmts_started = false; 
		  
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


  IF num_stmts_failed = 0 AND (current_stmt_index -1)= array_length(stmts,1) THEN
  	return true;
  else
  	return false;
  END IF;
  
END;
$$ language plpgsql;

GRANT EXECUTE on FUNCTION execute_parallel(stmts text[], num_parallel_thread int) TO public;

