CREATE TABLE public."dictionary" (
	"token" varchar NULL,
	replace_with varchar NULL,
	CONSTRAINT dictionary_pk PRIMARY KEY ("token")
);

CREATE TABLE public.tweet_token (
	tweet_id int8 NULL,
	"token" varchar NULL,
	CONSTRAINT tweet_token_pk PRIMARY KEY (tweet_id,"token")
);

ALTER TABLE public.tweet_token ADD CONSTRAINT tweet_token_fk FOREIGN KEY (tweet_id) REFERENCES public.tweet(tweet_id);
