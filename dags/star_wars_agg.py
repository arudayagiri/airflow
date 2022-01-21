from airflow.operators.postgres_operator import PostgresOperator
from airflow import DAG
from datetime import datetime, date
import requests
import json
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

output_agg_dir = "/opt/airflow/data/star_wars_data/agg_data/star_wars_agg.csv"

default_args = {
    'owner': 'Anvesh',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 21),
    'email': ['xxxx@xxxxx.com'],
    'email_on_failure': False,
    'postgres_conn_id': 'postgres_local'
}


# saving aggregated data in to csv file
def save_data(table_name_sql, output_file):
    conn = PostgresHook(postgres_conn_id='postgres_local').get_conn()
    cur = conn.cursor()
    SQL_STATEMENT = "COPY {0} TO STDOUT WITH CSV HEADER".format(table_name_sql)

    with open(output_file, 'w') as f:
        cur.copy_expert(SQL_STATEMENT, f)
        conn.close()


dag = DAG(
    'agg_star_wars',
    default_args=default_args,
    schedule_interval="17 14 * * *",
    template_searchpath='/opt/airflow/scripts/sql/star_wars/',
    catchup=False,
)

# Sensor poke ingestion pipeline once ingestion pipeline completes will start this dag

wait = ExternalTaskSensor(
    task_id="wait_for_ingestion",
    external_dag_id="ingest_star_wars",
    poke_interval=60,
    timeout=300,
    soft_fail=False,
    retries=2,
    external_task_id="ingestion_success",
    dag=dag
)

# Task to create table for aggregated data
Create_star_wars_agg_table = PostgresOperator(
    task_id='create_agg_table',
    sql='create_agg_table.sql',
    params={"table_name_sql": "star_wars_agg_data"},
    dag=dag
)

# task for to transform and create aggregated data
aggregation_of_data = PostgresOperator(
    task_id='aggregation_of_data',
    sql='star_wars_agg.sql',
    params={"people_table_name": "star_wars_people",
            "films_table_name": "star_wars_films",
            "agg_table_name": "star_wars_agg_data",
            },
    dag=dag
)
# task to save aggregated data in to csv
save_agg_data = PythonOperator(
    task_id='save_agg_data',
    python_callable=save_data,
    op_kwargs={"table_name_sql": "star_wars_agg_data", "output_file": output_agg_dir},
    dag=dag
)
# ordering of tasks to be executed

wait >> Create_star_wars_agg_table >> aggregation_of_data >> save_agg_data
