from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

import logging
import os

import pandas as pd
from datetime import datetime

# Подключение к базе данных PostgreSQL
postgres_hook = PostgresHook(postgres_conn_id="dwh-db")
conn = postgres_hook.get_conn()

# Функция для вставки данных в таблицу
def import_data_deal_info(table_name):

    engine = postgres_hook.get_sqlalchemy_engine()

    df = pd.read_csv(f"/files/{table_name}.csv", delimiter=",", encoding="cp1251")
    
    # Вставка данных в таблицу
    df.to_sql(table_name, engine, schema="rd", if_exists="append", index=False)

# Функция для вставки данных в таблицу
def import_data_product(table_name):

    engine = postgres_hook.get_sqlalchemy_engine()

    # Очистка существующих данных перед загрузкой новых данных
    with engine.connect() as conn:
        conn.execute(f'TRUNCATE TABLE rd."{table_name}";')

    df = pd.read_csv(f"/files/{table_name}.csv", delimiter=",", encoding="cp1251")
    
    
    # Вставка данных в таблицу
    df.to_sql(table_name, engine, schema="rd", if_exists="append", index=False)

default_args = {
    "owner": "amarganova",
    "start_date": datetime(2025, 1, 1),  
    "retries": 2,
}

with DAG(
    "import_to_rd",
    default_args=default_args,
    description="Импорт данных на слой rd",
    catchup=False,
    schedule="0 0 * * *" 
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    import_deal_info = PythonOperator(
        task_id="import_deal_info",
        python_callable=import_data_deal_info,
        op_kwargs={"table_name": "deal_info"}
    )

    import_product = PythonOperator(
        task_id="import_product",
        python_callable=import_data_product,
        op_kwargs={"table_name": "product"}
    )

    start >>  import_product 

