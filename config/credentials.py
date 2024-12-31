# config/credentials.py

# MongoDB configuration
MONGO_CONFIG = {
    "uri": "mongodb://etl_user:aP8fwfgftempRhkgGa9@3.251.75.195:27017/sales",
    "database": "sales",
    "collection": "sales"
}

# PostgreSQL configuration
POSTGRES_CONFIG = {
    "dbname": "sales_db2",
    "user": "ashok",
    "password": "ashok",
    "host": "rds-module.cnc6gugkeq4f.eu-west-1.rds.amazonaws.com",
    "port": "5432"
}
