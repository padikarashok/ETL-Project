
This project creates an ETL (Extract, Transform, Load) pipeline to move and prepare data from MongoDB and PostgreSQL OLTP systems into an OLAP data warehouse. 
The pipeline makes sure data is processed quickly and correctly.

How it works:
  1.From MongoDB to OLTP:
      Data is taken from MongoDB in small parts and loaded into a staging area in PostgreSQL OLTP.
      The data is cleaned and checked in this step.
  2.From OLTP to OLAP:
      Data from the OLTP system is transformed and loaded into the OLAP data warehouse.
      It updates important tables like users, products, and sales to be ready for reporting.
      This pipeline combines data from MongoDB and OLTP into one place, making it easy to create reports and analyze the data.
