from airflow import DAG
from datetime import datetime, date
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

default_args = {
    'owner': 'Anvesh',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 21),
    'email': ['xxxx@xxxxx.com'],
    'email_on_failure': False

}

dag = DAG(dag_id='transform_postcodes_data',
          default_args=default_args,
          schedule_interval="20 14 * * *",
          catchup=False)
# Sensor poke ingestion pipeline once ingestion pipeline completes will start this dag
wait = ExternalTaskSensor(
    task_id="raw_data_sensor",
    external_dag_id="ingest_raw_postcodes",
    poke_interval=60,
    timeout=300,
    soft_fail=False,
    retries=2,
    external_task_id="get_postcodes_data",
    dag=dag
)
# task for to do transformation of raw file
transform_postcode_data = BashOperator(
    task_id='transform_postcode_data',
    bash_command='python /opt/airflow/scripts/python/postcodes/transform_postcodes.py',
    dag=dag)

wait >> transform_postcode_data
