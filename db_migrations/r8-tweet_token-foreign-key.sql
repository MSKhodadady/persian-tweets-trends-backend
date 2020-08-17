ALTER TABLE public.tweet_token DROP CONSTRAINT tweet_token_fk;
ALTER TABLE public.tweet_token ADD CONSTRAINT tweet_token_fk FOREIGN KEY (tweet_id) REFERENCES public.tweet(tweet_id) ON DELETE CASCADE ON UPDATE CASCADE;

