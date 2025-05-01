import os
from SCRIPTS import DatabaseClient

def deploy_functions_from_folder(folder_path="FUNCTION"):
    """
    Reads all .sql files in `folder_path` (alphabetically) and executes each
    against the database, creating or replacing your PL/pgSQL functions.
    """
    sql_files = sorted(f for f in os.listdir(folder_path) if f.lower().endswith(".sql"))
    with DatabaseClient() as db:
        for fname in sql_files:
            full_path = os.path.join(folder_path, fname)
            with open(full_path, "r") as f:
                sql = f.read()
            db.execute(sql)
            print(f"Deployed {fname}")
