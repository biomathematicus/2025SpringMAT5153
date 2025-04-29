import os
import psycopg2
import numpy as np
import matplotlib.pyplot as plt

# 1) Fetch county poverty stats
conn = psycopg2.connect(
    dbname='postgres', user='postgres',
    password=os.getenv('PostgreSQL_PWD'),
    host='localhost', port='5432'
)
with conn.cursor() as cur:
    cur.execute('SELECT pct_poverty_5_17 FROM crdc_import."GetCountyPovertyStats"();')
    rows = cur.fetchall()

# Replace None with 0.0 (only 1 case "Richmond")
rates = [float(r[0] or 0) for r in rows]

plt.hist(rates, bins=30, alpha=0.7, edgecolor='black')
plt.title('Distribution of Poverty Rate (Ages 5–17) ')
plt.xlabel('Pct in Poverty (%)')
plt.ylabel('Number of Counties')
plt.show()
