import http.client
import json
import pandas as pd
from sqlalchemy import create_engine , text
import os
from dotenv import load_dotenv

load_dotenv()

def fetch_state_gas_prices(state):
    conn = http.client.HTTPSConnection("api.collectapi.com")

    headers = {
        "content-type": "application/json",
        'authorization': os.getenv('API_KEY'),
    }

    conn.request("GET", f"/gasPrice/stateUsaPrice?state={state}", headers=headers)

    res = conn.getresponse()
    data = json.loads(res.read().decode("utf-8"))
    data = data["result"]["cities"]
    conn.close()

    return data


def transform_cities(data):
    cities_df = pd.DataFrame(data)
    cities_df = cities_df.drop(columns="lowername")
    cities_df = cities_df.rename(columns={"name": "cities"})
    return cities_df





def load_cities(cities_df):

    DATABASE_NAME = os.getenv('DATABASE_NAME')
    DATABASE_USER = os.getenv('DATABASE_USER')
    DATABASE_PORT = os.getenv('DATABASE_PORT')
    DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
    DATABASE_HOST = os.getenv('DATABASE_HOST')

    engine = create_engine(f'postgresql+psycopg2://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}')

    with engine.connect() as conn:
        resort = conn.execute(text('select 1;'))
        for i in resort:
            print(i)

    cities_df.to_sql('cities',engine , if_exists='replace',index=False )
        
def main():
    state = "WA"
    data = fetch_state_gas_prices(state)
    cities_df = transform_cities(data)
    load_cities(cities_df)

    print('ETL process completed successfully.')


if __name__ == "__main__":
    main()  