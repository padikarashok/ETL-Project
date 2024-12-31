-- Table: sales_olap.dim_date

-- DROP TABLE IF EXISTS sales_olap.dim_date;

CREATE TABLE IF NOT EXISTS sales_olap.dim_date
(
    date_id integer NOT NULL DEFAULT nextval('sales_olap.dim_date_date_id_seq'::regclass),
    date_val date NOT NULL,
    year integer NOT NULL,
    month integer NOT NULL,
    day integer NOT NULL,
    hour integer NOT NULL,
    CONSTRAINT dim_date_pkey PRIMARY KEY (date_id),
    CONSTRAINT dim_date_date_val_hour_key UNIQUE (date_val, hour)
);



-- Table: sales_olap.dim_products


CREATE TABLE IF NOT EXISTS sales_olap.dim_products
(
    dim_product_id integer NOT NULL DEFAULT nextval('sales_olap.dim_products_dim_product_id_seq'::regclass),
    product_id bigint NOT NULL,
    brand character varying(255) COLLATE pg_catalog."default",
    category_id bigint,
    main_category character varying(255) COLLATE pg_catalog."default",
    sub_category character varying(255) COLLATE pg_catalog."default",
    sub_sub_category character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT dim_products_pkey PRIMARY KEY (dim_product_id),
    CONSTRAINT dim_products_product_id_uk UNIQUE (product_id)
)

-- Table: sales_olap.dim_users


CREATE TABLE IF NOT EXISTS sales_olap.dim_users
(
    dim_user_id integer NOT NULL DEFAULT nextval('sales_olap.dim_users_dim_user_id_seq'::regclass),
    user_id bigint,
    user_name character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT dim_users_pkey PRIMARY KEY (dim_user_id),
    CONSTRAINT dim_users_user_id_key UNIQUE (user_id)
)

-- Table: sales_olap.fact_sales


CREATE TABLE IF NOT EXISTS sales_olap.fact_sales
(
    fact_sales_id integer NOT NULL DEFAULT nextval('sales_olap.fact_sales_fact_sales_id_seq'::regclass),
    date_id integer NOT NULL,
    dim_user_id integer NOT NULL,
    dim_product_id integer NOT NULL,
    order_id bigint NOT NULL,
    price numeric(10,2),
    CONSTRAINT fact_sales_pkey PRIMARY KEY (fact_sales_id),
    CONSTRAINT fact_sales_date_id_fkey FOREIGN KEY (date_id)
        REFERENCES sales_olap.dim_date (date_id) 
    CONSTRAINT fact_sales_dim_product_id_fkey FOREIGN KEY (dim_product_id)
        REFERENCES sales_olap.dim_products (dim_product_id) 
    CONSTRAINT fact_sales_dim_user_id_fkey FOREIGN KEY (dim_user_id)
        REFERENCES sales_olap.dim_users (dim_user_id) 
)



CREATE TABLE sales_olap.etl_metadata (
    table_name VARCHAR(255) PRIMARY KEY,   -- Name of the source collection/table
    last_processed_id VARCHAR(24)         -- MongoDB _id (stored as string)
);
