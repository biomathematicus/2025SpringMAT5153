import os
import requests
import pandas as pd
import time

# Get API key at: https://api.census.gov/data/key_signup.html
# then, add CENSUS_API_KEY to your environment variables
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")

# Set year and month (month is unused in this dataset, but reserved for generality)
DATASET = '2020/dec/pl'
BASE_URL = f'https://api.census.gov/data/{DATASET}'
VARIABLES_URL = f'https://api.census.gov/data/{DATASET}/variables.json'

# Define tract
STATE = '48'       # Texas
COUNTY = '029'     # Bexar
TRACT = '110100'   # Example tract

def get_all_variables():
    response = requests.get(VARIABLES_URL)
    if response.status_code != 200:
        raise Exception(f"Could not fetch variables: {response.text}")
    variables = response.json()['variables']
    usable_vars = [var for var in variables if not var.startswith(('for', 'in'))]
    return usable_vars

def batch(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def fetch_data_for_variable_batch(variable_batch, state, county, tract):
    var_string = ','.join(variable_batch)
    params = {
        'get': var_string,
        'for': f'tract:{tract}',
        'in': f'state:{state} county:{county}',
        'key': CENSUS_API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve batch: {response.text}")
    data = response.json()
    return pd.DataFrame(data[1:], columns=data[0])

def fetch_all_data_per_tract(state, county, tract):
    variables = get_all_variables()
    all_results = []
    for variable_batch in batch(variables, 45):  # Keep under 50 to include geography fields
        try:
            df = fetch_data_for_variable_batch(variable_batch, state, county, tract)
            all_results.append(df)
            time.sleep(0.5)
        except Exception as e:
            print(f"Batch error: {e}")
    if not all_results:
        return pd.DataFrame()
    merged = pd.concat(all_results, axis=1)
    merged = merged.loc[:, ~merged.columns.duplicated()]
    return merged

if __name__ == '__main__':
    df = fetch_all_data_per_tract(STATE, COUNTY, TRACT)
    if not df.empty:
        print(df.head())
        df.to_csv(f'census_tract_{TRACT}_all_variables.csv', index=False)
    else:
        print("No data retrieved.")
