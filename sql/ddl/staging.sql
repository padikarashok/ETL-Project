-- Table: sales_oltp.sales_staging

-- DROP TABLE IF EXISTS sales_oltp.sales_staging;

CREATE TABLE IF NOT EXISTS sales_oltp.sales_staging
(
    id integer NOT NULL DEFAULT nextval('sales_oltp.sales_staging_id_seq'::regclass),
    mongo_id character varying(24) COLLATE pg_catalog."default",
    event_time timestamp without time zone,
    order_id bigint,
    product_id bigint,
    category_id bigint,
    category_code character varying(255) COLLATE pg_catalog."default",
    brand character varying(255) COLLATE pg_catalog."default",
    price numeric(10,2),
    user_id bigint,
    processed boolean DEFAULT false,
    CONSTRAINT sales_staging_pkey PRIMARY KEY (id),
    CONSTRAINT sales_staging_mongo_id_key UNIQUE (mongo_id)
)
