
DROP FUNCTION IF EXISTS execute_parallel(stmts text[], num_parallel_thread int);

drop extension dblink cascade;