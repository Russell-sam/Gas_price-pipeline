from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator

import http.client
import json
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()


def extract_cities(**kwargs):
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


def transform_cities(**kwargs):
    data = kwargs["ti"].xcom_pull(task_ids="extract")

    cities_df = pd.DataFrame(data)

    cities_df.drop(columns="lowername", inplace=True, errors="ignore")

    cities_df.rename(columns={"name": "cities"}, inplace=True)

    cities_df = cities_df.astype(object).where(pd.notna(cities_df), None)

    cities_records = cities_df.to_dict(orient="records")

    kwargs["ti"].xcom_push(key="transform", value=cities_records)


def load_cities(**kwargs):
    cities_records = kwargs["ti"].xcom_pull(task_ids="transform", key="transform")

    cities_df = pd.DataFrame(cities_records)

    DATABASE_NAME = os.getenv("DATABASE_NAME")
    DATABASE_USER = os.getenv("DATABASE_USER")
    DATABASE_PORT = os.getenv("DATABASE_PORT")
    DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
    DATABASE_HOST = os.getenv("DATABASE_HOST")

    engine = create_engine(
        f"postgresql+psycopg2://{DATABASE_USER}:{DATABASE_PASSWORD}@"
        f"{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
    )

    with engine.connect() as conn:
        result = conn.execute(text("select 1;"))
        for row in result:
            print(row)

    cities_df.to_sql("cities", engine, if_exists="replace", index=False)


with DAG(
    "gas_prices_etl_dag",
    start_date=datetime(2026, 6, 10),
    schedule=timedelta(minutes=1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_cities,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_cities,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_cities,
    )

    extract_task >> transform_task >> load_task
