-- Users Table
CREATE TABLE sales_oltp.users (
    user_id BIGINT PRIMARY KEY,
    user_name VARCHAR(255) -- Optional if you want to store user details later
);

-- Orders Table
CREATE TABLE sales_oltp.orders (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    event_time TIMESTAMP NOT NULL,
    FOREIGN KEY (user_id) REFERENCES sales_oltp.users(user_id)
);

-- Categories Table
CREATE TABLE sales_oltp.categories (
    category_id BIGINT PRIMARY KEY,
    category_code VARCHAR(255)
);

-- Products Table
CREATE TABLE sales_oltp.products (
    product_id BIGINT PRIMARY KEY,
    brand VARCHAR(255),
    category_id BIGINT NOT NULL,
    FOREIGN KEY (category_id) REFERENCES sales_oltp.categories(category_id)
);



-- Order_Items Table
CREATE TABLE sales_oltp.order_items (
    order_item_id SERIAL PRIMARY KEY, -- Auto-incremented ID for each order item
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES sales_oltp.orders(order_id),
    FOREIGN KEY (product_id) REFERENCES sales_oltp.products(product_id)
);

CREATE TABLE sales_oltp.etl_metadata (
    table_name VARCHAR(255) PRIMARY KEY,   -- Name of the source collection/table
    last_processed_id VARCHAR(24)         -- MongoDB _id (stored as string)
);