from scripts.execute_sql_file import execute_sql_file
from scripts.get_postgres_conn import get_postgres_conn

def add_indices_to_tc_data_table(conn_id: str) -> None:
    with get_postgres_conn(conn_id).connect() as conn:
        execute_sql_file(conn, '/usr/local/airflow/dags/sql/add_indices_to_tc_data_table.sql')
