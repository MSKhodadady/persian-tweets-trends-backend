ALTER TABLE public."user" ALTER COLUMN iscrawl SET DEFAULT true;

ALTER TABLE public."user" RENAME TO twitter_user;

ALTER TABLE public.twitter_user ADD created_at timestamptz NOT NULL;
