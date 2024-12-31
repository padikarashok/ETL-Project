# ETL Pipeline Project

This project creates an **ETL (Extract, Transform, Load)** pipeline to move and prepare data from **MongoDB** and **PostgreSQL OLTP** systems into an **OLAP data warehouse**. The pipeline ensures that data is processed quickly and correctly.

## How It Works

### 1. From MongoDB to OLTP
- Data is extracted from MongoDB in small parts (batches) and loaded into a **staging area** in PostgreSQL OLTP.
- The data is cleaned and validated during this step.

### 2. From OLTP to OLAP
- Data from the OLTP system is **transformed** and loaded into the OLAP data warehouse.
- Important tables like **users**, **products**, and **sales** are updated to prepare for reporting.
- This pipeline combines data from MongoDB and OLTP into one place, making it easy to generate reports and analyze data.

---
