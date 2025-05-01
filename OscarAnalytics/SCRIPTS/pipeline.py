# scripts/pipeline.py

import os, sys

# ─── prepend repo root (where FUNCTION/ lives) to module search path ───
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from FUNCTION.db_client               import DatabaseClient
from FUNCTION.generate_histogram      import generate_histogram_from_query

def main():
    # ensure your DB password env var is set
    os.environ.setdefault("PostgreSQL_PWD", os.getenv("PostgreSQL_PWD",""))

    with DatabaseClient() as db:
        sql = 'SELECT varCount FROM crdc_import."GetEnrollmentVariables"();'
        generate_histogram_from_query(
            db, sql,
            bins=30,
            title="School Enrollment Counts",
            xlabel="Students per School",
            ylabel="Number of Schools"
        )

if __name__ == "__main__":
    main()
