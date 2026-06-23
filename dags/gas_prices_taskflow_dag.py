from datetime import datetime, timedelta

import http.client
import json
import os

import pandas as pd
from airflow.decorators import dag, task
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


@dag(
    dag_id="gas_prices_taskflow_dag",
    start_date=datetime(2026, 6, 10),
    schedule=timedelta(minutes=1),
    catchup=False,
)
def gas_prices_etl():
    @task
    def extract():
        conn = http.client.HTTPSConnection("api.collectapi.com")

        headers = {
            "content-type": "application/json",
            "authorization": os.getenv("API_KEY"),
        }

        conn.request("GET", "/gasPrice/stateUsaPrice?state=WA", headers=headers)

        res = conn.getresponse()
        data = json.loads(res.read().decode("utf-8"))
        data = data["result"]["cities"]
        conn.close()

        return data

    @task
    def transform(data):
        cities_df = pd.DataFrame(data)

        cities_df.drop(columns="lowername", inplace=True, errors="ignore")

        cities_df.rename(columns={"name": "cities"}, inplace=True)

        cities_df = cities_df.astype(object).where(pd.notna(cities_df), None)

        return cities_df.to_dict(orient="records")

    @task
    def load(cities_records):
        cities_df = pd.DataFrame(cities_records)

        database_name = os.getenv("DATABASE_NAME")
        database_user = os.getenv("DATABASE_USER")
        database_port = os.getenv("DATABASE_PORT")
        database_password = os.getenv("DATABASE_PASSWORD")
        database_host = os.getenv("DATABASE_HOST")

        engine = create_engine(
            f"postgresql+psycopg2://{database_user}:{database_password}@"
            f"{database_host}:{database_port}/{database_name}"
        )

        with engine.connect() as conn:
            result = conn.execute(text("select 1;"))
            for row in result:
                print(row)

        cities_df.to_sql("cities", engine, if_exists="replace", index=False)

    load(transform(extract()))


gas_prices_etl()
