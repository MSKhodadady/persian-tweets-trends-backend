CREATE TABLE public."user" (
	username varchar NULL,
	iscrawl bool NULL,
	CONSTRAINT user_pk PRIMARY KEY (username)
);


CREATE TABLE public.tweet (
	tweet_id int8 NULL,
	tweet_text varchar NULL,
	username varchar NULL,
	tweet_time timestamptz NULL,
	CONSTRAINT tweet_pk PRIMARY KEY (tweet_id),
	CONSTRAINT tweet_fk FOREIGN KEY (username) REFERENCES public."user"(username) ON DELETE CASCADE ON UPDATE CASCADE
);