import os

host = os.getenv("PostgreSQL_HOST")
port = os.getenv("PostgreSQL_PORT")
dbname = os.getenv("PostgreSQL_DBNAME")
user = os.getenv("PostgreSQL_USER")
password = os.getenv("PostgreSQL_PWD")


db_connection_params = {
    "host": host,        # Or your DB host
    "port": port,             # Or your DB port
    "dbname": dbname, # Replace with your database name
    "user": user,    # Replace with your PostgreSQL username
    "password": password # Replace with your PostgreSQL password
}

print(db_connection_params)