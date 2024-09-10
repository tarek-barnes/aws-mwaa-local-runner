import os
import sys

#  Update Python PATH to expose helper functions
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator

#  Helper functions
from scripts.backup_tc_data_table import backup_tc_data_table
from scripts.add_redeem_id_column_to_tc_data_table import add_redeem_id_column_to_tc_data_table
from scripts.add_indices_to_tc_data_table import add_indices_to_tc_data_table
from scripts.run_task_one_query import run_task_one_query

DB_CONN_ID = 'postgres_default'

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 0
}

with DAG(
        dag_id='thrive_takehome_task_one',
        default_args=default_args,
        description='Add column to TC_DATA table, assign TRANS_ID of oldest unassigned spent/expired transactions for the same customer.',
        schedule_interval=None,  # Manually
        start_date=None,
        tags=['task_one']
) as dag:

    backup_tc_data_table = PythonOperator(
        task_id='backup_tc_data_table',
        python_callable=backup_tc_data_table,
        op_kwargs={
            'conn_id': DB_CONN_ID
        }
    )

    add_redeem_id_column_to_tc_data_table = PythonOperator(
        task_id='add_redeem_id_column_to_tc_data_table',
        python_callable=add_redeem_id_column_to_tc_data_table,
        op_kwargs={
            'conn_id': DB_CONN_ID
        }
    )

    # to do: test if this actually helps / iterate on permutations of indices
    add_indices_to_tc_data_table = PythonOperator(
        task_id='add_indices_to_tc_data_table',
        python_callable=add_indices_to_tc_data_table,
        op_kwargs={
            'conn_id': DB_CONN_ID
        }
    )

    run_task_one_query = PythonOperator(
        task_id='run_task_one_query',
        python_callable=run_task_one_query,
        op_kwargs={
            'conn_id': DB_CONN_ID
        }
    )

    (
        backup_tc_data_table
        >> add_redeem_id_column_to_tc_data_table
        >> add_indices_to_tc_data_table
        >> run_task_one_query
    )


