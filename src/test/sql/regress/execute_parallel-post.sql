
DROP FUNCTION IF EXISTS execute_parallel(_stmts text[], _num_parallel_thread int,_close_open_conn boolean,_user_connstr text, _contiune_after_stat_exception boolean);
	
drop extension dblink cascade;