# main_script.py

import psycopg2
from config.credentials import DB_CONFIG 
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_order_counts():
    """
    Validates that the distinct order_id counts match between staging and fact tables.
    """
    try:
        # Connect to the database using DB_CONFIG
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Query to count distinct order_id in sales_staging
        staging_query = "SELECT COUNT(DISTINCT order_id) FROM sales_oltp.sales_staging"
        cursor.execute(staging_query)
        staging_count = cursor.fetchone()[0]
        logging.info(f"Distinct order_id count in staging: {staging_count}")

        # Query to count distinct order_id in fact_sales
        fact_query = "SELECT COUNT(DISTINCT order_id) FROM sales_olap.fact_sales"
        cursor.execute(fact_query)
        fact_count = cursor.fetchone()[0]
        logging.info(f"Distinct order_id count in fact_sales: {fact_count}")

        # Validation
        if staging_count == fact_count:
            logging.info("Validation passed! Distinct order_id counts match.")
        else:
            logging.error(f"Validation failed! Staging count: {staging_count}, Fact count: {fact_count}")
            raise ValueError("Distinct order_id count mismatch between staging and fact tables!")

    except Exception as e:
        logging.error(f"Error during validation: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        logging.info("Database connection closed.")

# Run the validation
if __name__ == "__main__":
    validate_order_counts()
