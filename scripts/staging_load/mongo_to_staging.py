import logging
import time
from bson.objectid import ObjectId
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values
from config.credentials import MONGO_CONFIG, POSTGRES_CONFIG

# ------------------------------------------------------------------------------
# Configure logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ------------------------------------------------------------------------------
# MongoDB Connection
# ------------------------------------------------------------------------------
mongo_client = MongoClient(MONGO_CONFIG["uri"])
mongo_db = mongo_client[MONGO_CONFIG["database"]]
mongo_collection = mongo_db[MONGO_CONFIG["collection"]]

# ------------------------------------------------------------------------------
# PostgreSQL Connection
# ------------------------------------------------------------------------------
PG_CONN = psycopg2.connect(
    dbname=POSTGRES_CONFIG["dbname"],
    user=POSTGRES_CONFIG["user"],
    password=POSTGRES_CONFIG["password"],
    host=POSTGRES_CONFIG["host"],
    port=POSTGRES_CONFIG["port"]
)
PG_CURSOR = PG_CONN.cursor()

# ------------------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------------------
def get_last_processed_id():
    query = """
        SELECT last_processed_id
        FROM sales_oltp.etl_metadata
        WHERE table_name = 'sales_staging'
    """
    PG_CURSOR.execute(query)
    result = PG_CURSOR.fetchone()
    if result and result[0]:
        logging.info(f"Current stored last_processed_id in metadata: {result[0]}")
        return ObjectId(result[0])
    else:
        logging.info("No last_processed_id found in metadata (first run?).")
        return None

def update_last_processed_id(last_id):
    query = """
        INSERT INTO sales_oltp.etl_metadata (table_name, last_processed_id)
        VALUES ('sales_staging', %s)
        ON CONFLICT (table_name)
        DO UPDATE SET last_processed_id = EXCLUDED.last_processed_id
    """
    PG_CURSOR.execute(query, (str(last_id),))
    PG_CONN.commit()
    logging.info(f"Updated metadata table with last_processed_id: {last_id}")

def load_to_staging(batch_size=10000):
    while True:
        start_time = time.time()
        last_processed_id = get_last_processed_id()
        query_filter = {"_id": {"$gt": last_processed_id}} if last_processed_id else {}

        logging.info(f"Fetching a batch of up to {batch_size} records from Mongo...")
        batch = list(
            mongo_collection.find(query_filter)
                            .sort("_id", 1)
                            .limit(batch_size)
        )
        if not batch:
            logging.info("No more records to process. Exiting loop.")
            break

        logging.info(f"Batch size: {len(batch)}")
        logging.info(f"First ID in batch: {batch[0]['_id']} | Last ID in batch: {batch[-1]['_id']}")

        transform_start = time.time()
        staging_data = [
            (
                str(doc["_id"]),
                doc.get("event_time", "1970-01-01 00:00:00"),
                doc.get("order_id", -1),
                doc.get("product_id", -1),
                doc.get("category_id", -1),
                doc.get("category_code", "unknown"),
                doc.get("brand", "unknown"),
                float(doc.get("price", 0.0)),
                doc.get("user_id", -1)
            )
            for doc in batch
        ]
        transform_end = time.time()
        logging.info(f"Transformed data in {transform_end - transform_start:.2f} seconds.")

        load_start = time.time()
        insert_query = """
            INSERT INTO sales_oltp.sales_staging (
                mongo_id,
                event_time,
                order_id,
                product_id,
                category_id,
                category_code,
                brand,
                price,
                user_id
            )
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        execute_values(PG_CURSOR, insert_query, staging_data)
        PG_CONN.commit()
        load_end = time.time()
        logging.info(f"Loaded data into staging table in {load_end - load_start:.2f} seconds.")

        new_last_id = batch[-1]["_id"]
        update_last_processed_id(new_last_id)

        total_time = time.time() - start_time
        logging.info(f"Processed batch of {len(batch)} records in {total_time:.2f} seconds.\n")

def main():
    try:
        load_to_staging(batch_size=10000)
    finally:
        PG_CURSOR.close()
        PG_CONN.close()
        mongo_client.close()
        logging.info("Closed all database connections.")

if __name__ == "__main__":
    main()
