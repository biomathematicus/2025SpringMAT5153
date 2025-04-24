import os
import matplotlib.pyplot as plt
import psycopg2
import numpy as np

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
            # select * from "crdc_import"."GetEnrollmentVariables"();
            cur.execute('select * from "crdc_import"."GetEnrollmentVariables"();')
            # cur.execute("""
            #     SELECT
            #       SUM(tot_enr_m::integer)    AS total_males,
            #       SUM(tot_enr_f::integer)    AS total_females,
            #       SUM(tot_enr_m::integer + tot_enr_f::integer) AS total_students
            #     FROM enrollment;
            # """)
            return cur.fetchall()
    finally:
        conn.close()


if __name__ == '__main__':
    #males, females, total = fetch_enrollment_totals()
    rows = fetch_enrollment_totals()
    #print(f"Total male students:   {males}")
    #print(f"Total female students: {females}")
    #print(f"Overall total:         {total}")
    
    #Now , how to extract all members of a tuple
    n = 1 # this is the column number
    y = [x[n] for x in rows ]
    # If the field is numeric , you might want to convert it to a matrix
    z = np. asarray(y)
    # Plotting the histogram
    plt.hist(z, bins=30, alpha=0.7, color='blue', edgecolor='black')

    # Adding titles and labels
    plt.title('Histogram of Vector z')
    plt.xlabel('Value')
    plt.ylabel('Frequency')

    # Displaying the plot
    plt.show()    
