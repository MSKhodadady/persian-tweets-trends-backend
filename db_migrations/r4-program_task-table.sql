CREATE TABLE public.program_task (
	id serial NOT NULL,
	task_user varchar NULL,
	task_type varchar NULL,
	task_state varchar NULL,
	task_time timestamp NULL,
	CONSTRAINT program_task_pk PRIMARY KEY (id),
	CONSTRAINT program_task_fk FOREIGN KEY (task_user) REFERENCES public.twitter_user(username)
);
