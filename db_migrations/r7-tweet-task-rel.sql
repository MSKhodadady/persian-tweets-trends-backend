ALTER TABLE public.tweet ADD crawler_task int4 NULL;
ALTER TABLE public.tweet ADD CONSTRAINT tweet_fk_task FOREIGN KEY (crawler_task) REFERENCES public.program_task(id);

