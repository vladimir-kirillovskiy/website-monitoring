CREATE TABLE public.website_availability
(
	    website character varying(25) COLLATE pg_catalog."default" NOT NULL,
	    status_code integer,
	    response_time numeric,
	    regex_found boolean,
	    insert_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP
)
