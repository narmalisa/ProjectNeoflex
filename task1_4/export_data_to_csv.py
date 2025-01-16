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
        """, (start_time, 'Начало экспорта данных'))
        
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
        """, (end_time, 'Экспорт данных завершен успешно', log_id))
        
    conn.commit()
   
    cursor.close()


def export_to_csv():
    postgres_hook = PostgresHook(postgres_conn_id="postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()

    query = 'SELECT * FROM "DM".DM_F101_ROUND_F;' 
    df = pd.read_sql(query, engine)

    # Вывод текущего рабочего каталога
    current_directory = os.getcwd()
    logging.info(f'Текущий рабочий каталог: {current_directory}')

    try:
        df.to_csv("dm_f101_round_f.csv", index=False)
        logging.info('Файл успешно сохранен.')
    except Exception as e:
        logging.error(f'Ошибка при сохранении файла: {e}')


default_args = {
    "owner": "amarganova",
    "start_date": datetime(2025, 1, 15),  
    "retries": 2,
}

with DAG(
    "export_data",
    default_args=default_args,
    description="Экспорт данных в csv-файл",
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

    export_f101 = PythonOperator(
        task_id="export_to_csv",
        python_callable=export_to_csv
    )

    export_end = PythonOperator(
        task_id="log_export_end",
        python_callable=log_export_end,
        provide_context=True 
    )

    start >> export_start >> export_f101 >> export_end