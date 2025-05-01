import os
import psycopg2

class DatabaseClient:
    """
    A simple persistent PostgreSQL client for pipeline use.
    Maintains one connection and cursor until closed.
    """

    def __init__(self):
        # Load database configuration from environment variables
        self._config = {
            'dbname':   os.getenv('DB_NAME', 'postgres'),
            'user':     os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('PostgreSQL_PWD'),
            'host':     os.getenv('DB_HOST', 'localhost'),
            'port':     os.getenv('DB_PORT', '5432')
        }
        self.conn = None
        self.cur = None

    def connect(self):
        """Establishes the database connection and cursor."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**self._config)
            self.cur = self.conn.cursor()
        return self

    def query(self, sql, params=None):
        """
        Executes a SQL query and returns all fetched rows.
        :param sql: SQL string with optional placeholders
        :param params: tuple of parameters for placeholders
        """
        if self.cur is None:
            self.connect()
        self.cur.execute(sql, params or ())
        return self.cur.fetchall()

    def execute(self, sql, params=None):
        """
        Executes a SQL command (INSERT/UPDATE/DELETE).
        Commits immediately.
        """
        if self.cur is None:
            self.connect()
        self.cur.execute(sql, params or ())
        self.conn.commit()

    def close(self):
        """Closes cursor and connection."""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# Example usage in a pipeline script:
if __name__ == '__main__':
    db = DatabaseClient().connect()
    try:
        # Fetch county poverty stats for plotting
        rows = db.query('SELECT pct_poverty_5_17 FROM crdc_import."GetCountyPovertyStats"();')
        # ... pass `rows` to plotting functions ...
    finally:
        db.close()

# Save this as db_client.py and import DatabaseClient in your main pipeline.
