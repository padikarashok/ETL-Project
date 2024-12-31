import logging
import time
import psycopg2
from psycopg2.extras import execute_values
from config.credentials import POSTGRES_CONFIG  # Import the config file

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# PostgreSQL Connection
PG_CONN = psycopg2.connect(**POSTGRES_CONFIG)  # Unpack POSTGRES_CONFIG dictionary
PG_CURSOR = PG_CONN.cursor()

def process_staging_batch(batch_size=10000):
    """
    Processes up to 'batch_size' rows from sales_staging where processed = false,
    but with internal deduplication to avoid inserting the same key multiple times
    in a single ON CONFLICT statement.
    """
    # 1) Fetch unprocessed rows
    fetch_query = """
        SELECT id, mongo_id, event_time, order_id, product_id, category_id,
               category_code, brand, price, user_id
        FROM sales_oltp.sales_staging
        WHERE processed = false
        ORDER BY id
        LIMIT %s
    """
    PG_CURSOR.execute(fetch_query, (batch_size,))
    rows = PG_CURSOR.fetchall()

    if not rows:
        logging.info("No unprocessed rows in staging.")
        return 0

    # Keep track of staging IDs for marking processed
    processed_ids = []

    # Dedup structures
    # --------------------------------------------------
    # We'll store each unique user_id once
    user_data = {}  # key = user_id, value = True (or anything)
    
    # Categories: key = category_id, value = category_code
    category_data = {}
    
    # Products: key = product_id, value = (brand, category_id)
    product_data = {}
    
    # Orders: key = order_id, value = (user_id, event_time)
    order_data = {}
    
    # Order Items are often row-by-row, but if you want to deduplicate exact duplicates
    # you could use a set. For example:
    order_items_data = set()  # each item is (order_id, product_id, price)

    # 2) Loop through staging rows, build in-memory deduplicated sets/dicts
    for row in rows:
        staging_id = row[0]
        event_time = row[2] or '1970-01-01 00:00:00'
        order_id = row[3] or -1
        p_id = row[4] or -1
        c_id = row[5] or -1
        c_code = row[6] or 'unknown'
        brand = row[7] or 'unknown'
        price = row[8] or 0.0
        u_id = row[9] or -1

        processed_ids.append(staging_id)

        # a) Users: just store user_id in a dict
        user_data[u_id] = True

        # b) Categories: deduplicate by category_id
        if c_id in category_data:
            existing_code = category_data[c_id]
            if existing_code == 'unknown' and c_code != 'unknown':
                category_data[c_id] = c_code
        else:
            category_data[c_id] = c_code

        # c) Products: deduplicate by product_id
        if p_id in product_data:
            (existing_brand, existing_cat) = product_data[p_id]
            if existing_brand == 'unknown' and brand != 'unknown':
                product_data[p_id] = (brand, c_id)
        else:
            product_data[p_id] = (brand, c_id)

        # d) Orders: deduplicate by order_id
        if order_id in order_data:
            (existing_u, existing_time) = order_data[order_id]
            if existing_time < event_time:
                order_data[order_id] = (u_id, event_time)
        else:
            order_data[order_id] = (u_id, event_time)

        # e) Order_Items: we might want every row or deduplicate
        order_items_data.add((order_id, p_id, price))

    # 3) Bulk inserts with ON CONFLICT
    try:
        # Bulk Insert Users
        if user_data:
            user_list = [(uid,) for uid in user_data.keys()]
            insert_users_sql = """
                INSERT INTO sales_oltp.users (user_id)
                VALUES %s
                ON CONFLICT (user_id) DO NOTHING
            """
            execute_values(PG_CURSOR, insert_users_sql, user_list)

        # Bulk Insert Categories
        if category_data:
            cat_list = [(cid, ccode) for cid, ccode in category_data.items()]
            insert_cats_sql = """
                INSERT INTO sales_oltp.categories (category_id, category_code)
                VALUES %s
                ON CONFLICT (category_id) DO UPDATE
                    SET category_code = EXCLUDED.category_code
            """
            execute_values(PG_CURSOR, insert_cats_sql, cat_list)

        # Bulk Insert Products
        if product_data:
            prod_list = [(pid, b, catid) for pid, (b, catid) in product_data.items()]
            insert_prods_sql = """
                INSERT INTO sales_oltp.products (product_id, brand, category_id)
                VALUES %s
                ON CONFLICT (product_id) DO UPDATE
                    SET brand = EXCLUDED.brand,
                        category_id = EXCLUDED.category_id
            """
            execute_values(PG_CURSOR, insert_prods_sql, prod_list)

        # Bulk Insert Orders
        if order_data:
            order_list = [(oid, u, t) for oid, (u, t) in order_data.items()]
            insert_orders_sql = """
                INSERT INTO sales_oltp.orders (order_id, user_id, event_time)
                VALUES %s
                ON CONFLICT (order_id) DO NOTHING
            """
            execute_values(PG_CURSOR, insert_orders_sql, order_list)

        # Bulk Insert Order_Items
        if order_items_data:
            insert_oit_sql = """
                INSERT INTO sales_oltp.order_items (order_id, product_id, price)
                VALUES %s
            """
            execute_values(PG_CURSOR, insert_oit_sql, list(order_items_data))

        # Commit after successful bulk inserts
        PG_CONN.commit()

        # Mark these staging rows as processed
        update_query = """
            UPDATE sales_oltp.sales_staging
               SET processed = true
             WHERE id = ANY(%s)
        """
        PG_CURSOR.execute(update_query, (processed_ids,))
        PG_CONN.commit()
        logging.info(f"Marked {len(processed_ids)} rows as processed.")

    except Exception as e:
        logging.error(f"Error processing batch, rolling back. Error={e}")
        PG_CONN.rollback()
        return 0

    return len(processed_ids)


def main():
    try:
        while True:
            num_processed = process_staging_batch(batch_size=10000)
            if num_processed == 0:
                logging.info("No more rows to process. Exiting.")
                break
            else:
                logging.info(f"Processed {num_processed} rows this iteration.\n")
    finally:
        PG_CURSOR.close()
        PG_CONN.close()
        logging.info("Closed database connection.")


if __name__ == "__main__":
    main()
