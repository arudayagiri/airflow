from airflow.operators.postgres_operator import PostgresOperator
from airflow import DAG
from datetime import datetime, date
import requests
import json
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

source_people_url = 'https://swapi.dev/api/people/'
output_people_dir = "/opt/airflow/data/star_wars_data/raw_data/star_wars_people_raw.csv"
source_films_url = 'https://swapi.dev/api/films/'
output_films_dir = "/opt/airflow/data/star_wars_data/raw_data/star_wars_films_raw.csv"

default_args = {
    'owner': 'Anvesh',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 21),
    'email': ['xxxx@xxxxx.com'],
    'email_on_failure': False,
    'postgres_conn_id': 'postgres_local'
}


# getting api data and save it in to the csv file
# We are able to query max 10 records so need to query the data recursively

def get_star_wars_data(input_file, output_file):
    response = requests.get(input_file)
    data = json.loads(response.text)
    count = data['count']
    i = 1
    rows_count = 0
    df = pd.DataFrame()
    while rows_count != count:
        response = requests.get(input_file + '?page=' + str(i))
        data = json.loads(response.text)
        df_temp = pd.DataFrame(data['results'])
        df = df.append(df_temp)
        rows_count = df.shape[0]
        i = i + 1
    df.to_csv(output_file, index=False)


# loading data in to postgres sql db
# using postgres hook  copying data in bulk to the db instead of writing record by record
# truncating table every time to overwrite the data

def load_data(table_name_sql, input_file):
    conn = PostgresHook(postgres_conn_id='postgres_local').get_conn()
    cur = conn.cursor()
    SQL_STATEMENT = "truncate {0} ; COPY {0} FROM STDIN WITH DELIMITER AS E'\\,' CSV header ".format(table_name_sql)

    with open(input_file, 'r+') as f:
        cur.copy_expert(SQL_STATEMENT, f)
        f.truncate()
        conn.commit()


# defining dag
dag = DAG(
    'ingest_star_wars',
    default_args=default_args,
    schedule_interval="17 14 * * *",
    template_searchpath='/opt/airflow/scripts/sql/star_wars/',
    catchup=False,
)
# Task for ingest raw people api data using python operator
# saving the data in csv format
get_raw_star_wars_people_data = PythonOperator(
    task_id='get_raw_star_wars_people_data',
    python_callable=get_star_wars_data,
    op_kwargs={'output_file': output_people_dir, 'input_file': source_people_url},
    dag=dag
)

# Task for Creating table for people api data
Create_star_wars_people_table = PostgresOperator(
    task_id='create_people_table',
    sql='create_people_table.sql',
    params={"table_name_sql": "star_wars_people"},
    dag=dag
)

# Task for Inserting the people data in to table
insert_star_wars_people_data = PythonOperator(
    task_id='Insert_raw_people_data',
    python_callable=load_data,
    op_kwargs={"table_name_sql": "star_wars_people", "input_file": output_people_dir},
    dag=dag
)

# Task for ingest raw films api data using python operator
# saving the data in csv format
get_raw_star_wars_films_data = PythonOperator(
    task_id='get_raw_star_wars_films_data',
    python_callable=get_star_wars_data,
    op_kwargs={'output_file': output_films_dir, 'input_file': source_films_url},
    dag=dag
)

# Task for Creating table for films api data
Create_star_wars_films_table = PostgresOperator(
    task_id='create_films_table',
    sql='create_films_table.sql',
    params={"table_name_sql": "star_wars_films"},
    dag=dag
)

# ask for Inserting the films' data in to table
insert_star_wars_films_data = PythonOperator(
    task_id='Insert_raw_films_data',
    python_callable=load_data,
    op_kwargs={"table_name_sql": "star_wars_films", "input_file": output_films_dir},
    dag=dag
)
# Created a dummy to identify both apis ingestion successfully completed
# and also to send the information to external sensor
ingestion_success = DummyOperator(
    task_id='ingestion_success',
    trigger_rule="none_failed",
    dag=dag
)

# Ordering of tasks to be executed

get_raw_star_wars_films_data >> Create_star_wars_films_table >> insert_star_wars_films_data

get_raw_star_wars_people_data >> Create_star_wars_people_table >> insert_star_wars_people_data

[insert_star_wars_films_data, insert_star_wars_people_data] >> ingestion_success
