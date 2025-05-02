import os
import requests
import pandas as pd

# Replace this with your actual Census API key
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")

# Example parameters: 2020 Decennial Census (PL 94-171), Texas (state=48), Bexar County (county=029)
BASE_URL = 'https://api.census.gov/data/2020/dec/pl'
PARAMS = {
    'get': 'NAME,P1_001N',  # Total population
    'for': 'tract:*',
    'in': 'state:48 county:029',
    'key': CENSUS_API_KEY
}

def fetch_census_tract_data():
    response = requests.get(BASE_URL, params=PARAMS)
    if response.status_code != 200:
        raise Exception(f"Request failed with status code {response.status_code}: {response.text}")
    
    data = response.json()
    headers = data[0]
    records = data[1:]
    
    df = pd.DataFrame(records, columns=headers)
    return df

if __name__ == '__main__':
    try:
        df = fetch_census_tract_data()
        print(df.head())
        df.to_csv('census_tract_data.csv', index=False)
    except Exception as e:
        print(f"Error fetching data: {e}")
