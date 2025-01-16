from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

import logging
import os

import pandas as pd
from datetime import datetime

# Подключение к базе данных PostgreSQL
postgres_hook = PostgresHook(postgres_conn_id="postgres-db")
conn = postgres_hook.get_conn()

# Функция для логирования начала выгрузки данных из БД
def log_export_start(**kwargs):
    cursor = conn.cursor()
    start_time = datetime.now()
    
    cursor.execute("""
            INSERT INTO "LOGS".EXPORT_IMPORT_LOG (start_time, start_message)
            VALUES (%s, %s) RETURNING log_id;
        """, (start_time, 'Начало импорта данных'))
        
    log_id = cursor.fetchone()[0]
    conn.commit()
        
    # Сохранение log_id для дальнейшего использования
    kwargs['ti'].xcom_push(key='log_id', value=log_id)
    cursor.close()

# Функция для логирования окончания ETL процесса
def log_export_end(**kwargs):
    cursor = conn.cursor()

    end_time = datetime.now()
    
    log_id = kwargs['ti'].xcom_pull(key='log_id')
    
   
    cursor.execute("""
            UPDATE "LOGS".EXPORT_IMPORT_LOG 
            SET end_time = %s, end_message = %s 
            WHERE log_id = %s;
        """, (end_time, 'Импорт данных завершен успешно', log_id))
        
    conn.commit()
   
    cursor.close()

# Функция для вставки данных в таблицу
def import_data(table_name):

    engine = postgres_hook.get_sqlalchemy_engine()

    # Очистка существующих данных перед загрузкой новых данных
    with engine.connect() as conn:
        conn.execute(f'TRUNCATE TABLE "DM"."{table_name}";')

    df = pd.read_csv(f"/files/{table_name}.csv", delimiter=",")
    
    
    # Вставка данных в таблицу
    df.to_sql(table_name, engine, schema="DM", if_exists="append", index=False)

default_args = {
    "owner": "amarganova",
    "start_date": datetime(2025, 1, 1),  
    "retries": 2,
}

with DAG(
    "import_data",
    default_args=default_args,
    description="Импорт 101 формы",
    catchup=False,
    schedule="0 0 * * *" 
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    export_start = PythonOperator(
        task_id="log_export_start",
        python_callable=log_export_start,
        provide_context=True 
    )

    import_f101_v2 = PythonOperator(
        task_id="import_f101_v2",
        python_callable=import_data,
        op_kwargs={"table_name": "dm_f101_round_f_v2"}
    )

    export_end = PythonOperator(
        task_id="log_export_end",
        python_callable=log_export_end,
        provide_context=True 
    )

    start >> export_start >>  import_f101_v2 >> export_end