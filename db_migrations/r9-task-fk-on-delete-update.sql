ALTER TABLE public.program_task DROP CONSTRAINT program_task_fk;
ALTER TABLE public.program_task ADD CONSTRAINT program_task_fk FOREIGN KEY (task_user) REFERENCES public.twitter_user(username) ON DELETE CASCADE ON UPDATE CASCADE;

