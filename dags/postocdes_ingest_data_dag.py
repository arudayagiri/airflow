from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import requests
import zipfile
import io

source_url = 'https://www.doogal.co.uk/files/postcodes.zip'
output_dir = "/opt/airflow/data/postcodes_data/raw_data/" + str(date.today())

default_args = {
    'owner': 'Anvesh',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 21),
    'email': ['xxxx@xxxxx.com'],
    'email_on_failure': False,


}


# Extract the data from zip file and save it as a csv file
# if we have other formats save it in another folder
def get_postcode_data(input_file, output_file, **kwargs):
    r = requests.get(input_file)

    z = zipfile.ZipFile(io.BytesIO(r.content))
    list_file_names = z.namelist()

    for fileName in list_file_names:
        # Check filename endswith csv
        if fileName.endswith('.csv'):
            # Extract a single file from zip
            z.extract(fileName, output_file)
        else:
            print("The wrong format files stored in wrong data folder ")
            z.extract(fileName, output_file + "-Wrong data/")


dag = DAG(
    'ingest_raw_postcodes',
    default_args=default_args,
    schedule_interval="20 14 * * *",
    catchup=False,
)
# Task for extracting data from a  zip and saving it into a folder in csv format
get_postcode_data = PythonOperator(
    task_id='get_postcodes_data',
    python_callable=get_postcode_data,
    op_kwargs={'output_file': output_dir, 'input_file': source_url},
    dag=dag
)
