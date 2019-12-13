/*  
	queue table
	- I only added the fields that was relevant to 
	  run my code.
*/
CREATE TABLE IF NOT EXISTS tasks  (
	id SERIAL PRIMARY KEY,
	task_type varchar(40),
	exec_time timestamp with time zone,
	state varchar(10)
);

CREATE INDEX task_idx_exec_time_state ON tasks (exec_time, task_type);
