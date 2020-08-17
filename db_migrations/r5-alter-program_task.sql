ALTER TABLE public.program_task RENAME COLUMN task_time TO created_at;

ALTER TABLE public.program_task ADD crawl_since timestamp NULL;
ALTER TABLE public.program_task ADD crawl_until timestamp NULL;

ALTER TABLE public.program_task ALTER COLUMN created_at SET DEFAULT NOW();
