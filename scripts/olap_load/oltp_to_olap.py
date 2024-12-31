import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from config.credentials import POSTGRES_CONFIG  # Import PostgreSQL configuration

# ------------------------------------------------------------------------------
# Configure Logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# PostgreSQL Connection
# ------------------------------------------------------------------------------
conn = psycopg2.connect(**POSTGRES_CONFIG)
cur = conn.cursor()

# ------------------------------------------------------------------------------
# Dimension Bulk Loading
# ------------------------------------------------------------------------------

def load_dim_users_bulk(chunk_size=10000):
    """
    Bulk upserts user data from OLTP to OLAP dimension table.
    """
    logger.info("Starting bulk load for dim_users...")
    offset = 0
    total_inserted = 0

    while True:
        cur.execute("""
            SELECT user_id, user_name
            FROM sales_oltp.users
            ORDER BY user_id
            LIMIT %s OFFSET %s
        """, (chunk_size, offset))
        rows = cur.fetchall()
        if not rows:
            break

        user_data = [(r[0], r[1]) for r in rows]
        upsert_sql = """
            INSERT INTO sales_olap.dim_users (user_id, user_name)
            VALUES %s
            ON CONFLICT (user_id)
            DO UPDATE SET user_name = EXCLUDED.user_name
        """
        execute_values(cur, upsert_sql, user_data, page_size=chunk_size)
        conn.commit()

        offset += chunk_size
        total_inserted += len(rows)
        logger.info(f"Upserted {len(rows)} users, offset now {offset}.")

    logger.info(f"Bulk load for dim_users complete. Total upserted: {total_inserted}")


def load_dim_products_bulk(chunk_size=10000):
    """
    Bulk upserts product data from OLTP to OLAP dimension table.
    """
    logger.info("Starting bulk load for dim_products...")
    offset = 0
    total_inserted = 0

    while True:
        cur.execute("""
            SELECT p.product_id, p.brand, c.category_id, c.category_code
            FROM sales_oltp.products p
            JOIN sales_oltp.categories c ON p.category_id = c.category_id
            ORDER BY p.product_id
            LIMIT %s OFFSET %s
        """, (chunk_size, offset))
        rows = cur.fetchall()
        if not rows:
            break

        product_data = []
        for (product_id, brand, category_id, category_code) in rows:
            parts = category_code.split(".")
            main_category = parts[0] if len(parts) > 0 else "unknown"
            sub_category = parts[1] if len(parts) > 1 else ""
            sub_sub_category = parts[2] if len(parts) > 2 else ""
            product_data.append((product_id, brand, category_id, main_category, sub_category, sub_sub_category))

        upsert_sql = """
            INSERT INTO sales_olap.dim_products (
                product_id, brand, category_id,
                main_category, sub_category, sub_sub_category
            )
            VALUES %s
            ON CONFLICT (product_id)
            DO UPDATE SET
                brand = EXCLUDED.brand,
                category_id = EXCLUDED.category_id,
                main_category = EXCLUDED.main_category,
                sub_category = EXCLUDED.sub_category,
                sub_sub_category = EXCLUDED.sub_sub_category
        """
        execute_values(cur, upsert_sql, product_data, page_size=chunk_size)
        conn.commit()

        offset += chunk_size
        total_inserted += len(rows)
        logger.info(f"Upserted {len(rows)} products, offset now {offset}.")

    logger.info(f"Bulk load for dim_products complete. Total upserted: {total_inserted}")


# ------------------------------------------------------------------------------
# Dimension Caching for Fact Table
# ------------------------------------------------------------------------------

def load_dim_date(order_timestamp: datetime, date_cache: dict) -> int:
    """
    Fetch or insert a row in dim_date, returning date_id.
    """
    date_val = order_timestamp.date()
    hour = order_timestamp.hour
    key = (date_val, hour)

    if key in date_cache:
        return date_cache[key]

    cur.execute("""
        INSERT INTO sales_olap.dim_date (date_val, year, month, day, hour)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date_val, hour) DO NOTHING
        RETURNING date_id
    """, (date_val, date_val.year, date_val.month, date_val.day, hour))
    result = cur.fetchone()

    if result:
        date_cache[key] = result[0]
        return result[0]

    cur.execute("""
        SELECT date_id
        FROM sales_olap.dim_date
        WHERE date_val = %s AND hour = %s
    """, (date_val, hour))
    date_id = cur.fetchone()[0]
    date_cache[key] = date_id
    return date_id

# ------------------------------------------------------------------------------
# Incremental Fact Table Loading
# ------------------------------------------------------------------------------

def load_fact_sales_incremental_bulk_with_caching(chunk_size=10000):
    """
    Incrementally load fact_sales in chunks with caching.
    """
    logger.info("Starting incremental load for fact_sales...")

    cur.execute("""
        SELECT last_processed_id
        FROM sales_oltp.etl_metadata
        WHERE table_name = 'fact_sales'
    """)
    result = cur.fetchone()
    last_loaded_order_id = int(result[0]) if result and result[0] else 0
    logger.info(f"Last loaded order_id = {last_loaded_order_id}")

    user_cache = {}
    product_cache = {}
    date_cache = {}
    total_inserted = 0
    current_max_id = last_loaded_order_id

    while True:
        cur.execute("""
            SELECT o.order_id, o.user_id, o.event_time, oi.product_id, oi.price
            FROM sales_oltp.orders o
            JOIN sales_oltp.order_items oi ON o.order_id = oi.order_id
            WHERE o.order_id > %s
            ORDER BY o.order_id
            LIMIT %s
        """, (current_max_id, chunk_size))
        rows = cur.fetchall()
        if not rows:
            break

        logger.info(f"Fetched {len(rows)} order rows (order_id > {current_max_id}), processing...")

        user_ids = {user_id for (_, user_id, _, _, _) in rows}
        product_ids = {product_id for (_, _, _, product_id, _) in rows}

        if user_ids:
            cur.execute("""
                SELECT user_id, dim_user_id
                FROM sales_olap.dim_users
                WHERE user_id IN %s
            """, (tuple(user_ids),))
            user_cache.update({row[0]: row[1] for row in cur.fetchall()})

        if product_ids:
            cur.execute("""
                SELECT product_id, dim_product_id
                FROM sales_olap.dim_products
                WHERE product_id IN %s
            """, (tuple(product_ids),))
            product_cache.update({row[0]: row[1] for row in cur.fetchall()})

        fact_data = []
        for (order_id, user_id, event_time, product_id, price) in rows:
            date_id = load_dim_date(order_timestamp=event_time, date_cache=date_cache)
            dim_user_id = user_cache.get(user_id)
            dim_product_id = product_cache.get(product_id)

            if dim_user_id and dim_product_id:
                fact_data.append((date_id, dim_user_id, dim_product_id, order_id, price))
                current_max_id = max(current_max_id, order_id)

        if fact_data:
            insert_sql = """
                INSERT INTO sales_olap.fact_sales (
                    date_id, dim_user_id, dim_product_id, order_id, price
                )
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            execute_values(cur, insert_sql, fact_data, page_size=1000)
            conn.commit()
            total_inserted += len(fact_data)
            logger.info(f"Inserted {len(fact_data)} rows into fact_sales.")

        if len(rows) < chunk_size:
            break

    cur.execute("""
        INSERT INTO sales_oltp.etl_metadata (table_name, last_processed_id)
        VALUES ('fact_sales', %s)
        ON CONFLICT (table_name)
        DO UPDATE SET last_processed_id = EXCLUDED.last_processed_id
    """, (current_max_id,))
    conn.commit()
    logger.info(f"Fact load complete. Total rows inserted: {total_inserted}. Final max_order_id: {current_max_id}")

# ------------------------------------------------------------------------------
# Orchestrator
# ------------------------------------------------------------------------------

def main():
    logger.info("Starting dimension load...")
    load_dim_users_bulk()
    load_dim_products_bulk()
    logger.info("Dimension load complete.")

    logger.info("Starting fact table incremental load...")
    load_fact_sales_incremental_bulk_with_caching()
    logger.info("Fact load complete.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"Unexpected error in main: {e}")
    finally:
        cur.close()
        conn.close()
        logger.info("Closed Postgres connection.")
