import os
import psycopg2

# load your DB password from env (or set PWD directly)
PWD = os.getenv('PostgreSQL_PWD')

POSTGRES_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': PWD,
    'host': 'localhost',
    'port': '5432'
}
# fetch data from "enrollment" table and cast strings to integer to utilizethe sum func
def fetch_enrollment_totals():
    """Returns (total_males, total_females, total_students) from the enrollment table."""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  SUM(tot_enr_m::integer)    AS total_males,
                  SUM(tot_enr_f::integer)    AS total_females,
                  SUM(tot_enr_m::integer + tot_enr_f::integer) AS total_students
                FROM enrollment;
            """)
            return cur.fetchone()
    finally:
        conn.close()


if __name__ == '__main__':
    males, females, total = fetch_enrollment_totals()
    print(f"Total male students:   {males}")
    print(f"Total female students: {females}")
    print(f"Overall total:         {total}")
