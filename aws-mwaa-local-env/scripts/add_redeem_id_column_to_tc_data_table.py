import logging

from sqlalchemy.exc import ProgrammingError

from scripts.execute_sql_file import execute_sql_file
from scripts.get_postgres_conn import get_postgres_conn

def add_redeem_id_column_to_tc_data_table(conn_id: str) -> None:
    with get_postgres_conn(conn_id).connect() as conn:
        try:
            execute_sql_file(conn, '/usr/local/airflow/dags/sql/add_redeem_id_column_to_tc_data_table.sql')
        except ProgrammingError as e:
            logging.error(f"It seems like this column already exists. Here's the error message: {e}")
