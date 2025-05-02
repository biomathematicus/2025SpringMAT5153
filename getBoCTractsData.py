import os
import requests
import pandas as pd
import time

# Replace this with your actual Census API key
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")

# Target census tracts as tuples of (state_fips, county_fips, tract_code)
TRACTS = [
    ('48', '029', '110100'),
    ('48', '029', '110300'),
    ('48', '029', '110500'),
]

# Variables to retrieve. For example: Total population, Hispanic/Latino population, White alone
VARIABLES = 'NAME,P1_001N,P2_002N,P2_005N'
# See list of variables at:  https://api.census.gov/data/2020/dec/pl/variables.json

BASE_URL = 'https://api.census.gov/data/2020/dec/pl'
"""
The dec/pl dataset refers to the Decennial Census Redistricting Data (Public Law 94-171). This dataset is produced every 10 years and contains population counts by race, Hispanic origin, and voting age for redistricting purposes. It is a complete enumeration rather than a sample and provides the most accurate population counts at the smallest geographic levels, such as blocks and tracts.
"""

def fetch_tract_data(state, county, tract):
    params = {
        'get': VARIABLES,
        'for': f'tract:{tract}',
        'in': f'state:{state} county:{county}',
        'key': CENSUS_API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code != 200:
        raise Exception(f"Failed for tract {tract}: {response.text}")
    data = response.json()
    return pd.DataFrame(data[1:], columns=data[0])

def fetch_selected_tracts(tract_list):
    results = []
    for state, county, tract in tract_list:
        try:
            df = fetch_tract_data(state, county, tract)
            results.append(df)
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"Error retrieving {tract}: {e}")
    if results:
        return pd.concat(results, ignore_index=True)
    else:
        return pd.DataFrame()

if __name__ == '__main__':
    df = fetch_selected_tracts(TRACTS)
    if not df.empty:
        print(df.head())
        df.to_csv('selected_census_tracts.csv', index=False)
    else:
        print("No data retrieved.")
