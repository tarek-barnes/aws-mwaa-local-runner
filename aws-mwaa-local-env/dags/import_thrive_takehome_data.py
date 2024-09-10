# TO DO
# - figure out a better way to reference SQL files (rather than typing out abs paths)
# - clean up files, make sure no libs are being called if not used
# - add docstrings to funcs

import os
import sys

#  Update Python PATH to expose helper functions
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator

#  Helper functions
from scripts.convert_xlsx_to_csv import convert_xlsx_to_csv
from scripts.download_public_file_from_s3 import download_public_file_from_s3
from scripts.load_csv_schemas_to_db import load_csv_schemas_to_db
from scripts.load_csvs_to_db import load_csvs_to_db
# from scripts.get_list_of_csv_filepaths_to_load_to_db import get_list_of_csv_filepaths_to_load_to_db

DB_CONN_ID = 'postgres_default'
S3_PUBLIC_URL = 'https://thrivemarket-candidate-test.s3.amazonaws.com/tc_raw_data.xlsx'
RAW_DATA_DESTINATION_FILEPATH = '/tmp/RAW-tc_raw_data.xlsx'
STAGING_DATA_DESTINATION_PATH = '/tmp/staging'

default_args = {
	'owner': 'data_engineering',
	'depends_on_past': False,
	'retries': 1
}

with DAG(
	dag_id='import_thrive_takehome_data',
	default_args=default_args,
	description='Download XLSX file from public S3 URL, convert to multiple CSVs, and persist CSVs to tables in Postgres DB.',
	schedule_interval=None,  # Manually
	start_date=None,
	tags=['s3', 'public', 'import']
) as dag:

	download_public_file_task = PythonOperator(
		task_id='download_public_file_from_s3',
		python_callable=download_public_file_from_s3,
		op_kwargs={
			's3_public_url': S3_PUBLIC_URL,
			'destination_file_path': RAW_DATA_DESTINATION_FILEPATH
		}
	)

	convert_xlsx_to_csv = PythonOperator(
        task_id='convert_xlsx_to_csv',
        python_callable=convert_xlsx_to_csv,
		op_kwargs={
			'xlsx_path': RAW_DATA_DESTINATION_FILEPATH,
			'csv_dir': STAGING_DATA_DESTINATION_PATH
		}
    )

	load_csv_schemas_to_db = PythonOperator(
        task_id='load_csv_schemas_to_db',
        python_callable=load_csv_schemas_to_db,
        op_kwargs={
        	'conn_id': DB_CONN_ID
        }
    )

	load_csvs_to_db = PythonOperator(
        task_id='load_csvs_to_db',
        python_callable=load_csvs_to_db,
        op_kwargs={
            'conn_id': DB_CONN_ID,
            'staging_destination_dir': STAGING_DATA_DESTINATION_PATH
        }
    )

	(
        download_public_file_task
        >> convert_xlsx_to_csv
        >> load_csv_schemas_to_db
        >> load_csvs_to_db
    )
