from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

import pandas as pd
from datetime import datetime
import time

# Функция для логирования начала ETL процесса
def log_etl_start(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="postgres-db")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    start_time = datetime.now()
    
    cursor.execute("""
        INSERT INTO "LOGS".etl_log (start_time, message)
        VALUES (%s, %s) RETURNING log_id;
        """, (start_time, 'ETL process started'))
    
    log_id = cursor.fetchone()[0]
    conn.commit()
    
    # Сохранение log_id для дальнейшего использования
    kwargs['ti'].xcom_push(key='log_id', value=log_id)

# Функция для логирования окончания ETL процесса
def log_etl_end(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="postgres-db")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    end_time = datetime.now()
    
    log_id = kwargs['ti'].xcom_pull(key='log_id')
    
    cursor.execute("""
        UPDATE "LOGS".etl_log 
        SET end_time = %s 
        WHERE log_id = %s;
        """, (end_time, log_id))
    
    conn.commit()

# Функция для преобразования колонок с датами в единый формат
def convert_date_columns(df):
    date_columns = df.select_dtypes(include=['object']).columns
    
    for col in date_columns:
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        except Exception as e:
            print(f"Ошибка при конвертации колонки {col}: {e}")
    
    return df

# Функция для вставки данных в таблицу
def load_data(table_name):
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()

    # Очистка существующих данных перед загрузкой новых данных
    with engine.connect() as conn:
        conn.execute(f'TRUNCATE TABLE "DS"."{table_name.upper()}";')

    df = pd.read_csv(f"/files/{table_name}.csv", delimiter=";")
    
    # Обработка колонок с датами
    df = convert_date_columns(df)
    
    # Вставка данных в таблицу
    df.to_sql(table_name.upper(), engine, schema="DS", if_exists="append", index=False)

def load_data_2(table_name):
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()

    # Очистка существующих данных перед загрузкой новых данных
    with engine.connect() as conn:
        conn.execute(f'TRUNCATE TABLE "DS"."{table_name.upper()}";')

    df = pd.read_csv(f"/files/{table_name}.csv", delimiter=";", encoding="cp1252")
    
    # Обработка колонок с датами
    df = convert_date_columns(df)
    
    # Вставка данных в таблицу
    df.to_sql(table_name.upper(), engine, schema="DS", if_exists="append", index=False)

default_args = {
    "owner": "amarganova",
    "start_date": datetime(2025, 1, 1),  
    "retries": 2,
}

with DAG(
    "load_data",
    default_args=default_args,
    description="Загрузка банковских данных в DS",
    catchup=False,
    schedule="0 0 * * *" 
) as dag:
    
    start = DummyOperator(
        task_id="start"
    )

    etl_start = PythonOperator(
        task_id="log_etl_start",
        python_callable=log_etl_start,
        provide_context=True 
    )

    ft_balance_f = PythonOperator(
        task_id="insert_ft_balance_f",
        python_callable=load_data,
        op_kwargs={"table_name": "ft_balance_f"}
    )

    ft_posting_f = PythonOperator(
        task_id="insert_ft_posting_f",
        python_callable=load_data,
        op_kwargs={"table_name": "ft_posting_f"}
    )

    md_account_d = PythonOperator(
        task_id="insert_md_account_d",
        python_callable=load_data,
        op_kwargs={"table_name": "md_account_d"}
    )

    md_currency_d = PythonOperator(
        task_id="insert_md_currency_d",
        python_callable=load_data_2,
        op_kwargs={"table_name": "md_currency_d"}
    )

    md_exchange_rate_d = PythonOperator(
        task_id="insert_md_exchange_rate_d",
        python_callable=load_data,
        op_kwargs={"table_name": "md_exchange_rate_d"}
    )

    md_ledger_account_s = PythonOperator(
        task_id="insert_md_ledger_account_s",
        python_callable=load_data,
        op_kwargs={"table_name": "md_ledger_account_s"}
    )

    
    etl_end = PythonOperator(
        task_id="log_etl_end",
        python_callable=log_etl_end,
        provide_context=True  
    )

    
    start >> etl_start >> [ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s] >> etl_end
