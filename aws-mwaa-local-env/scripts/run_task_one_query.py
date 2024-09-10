from scripts.execute_sql_file import execute_sql_file
from scripts.get_postgres_conn import get_postgres_conn

def run_task_one_query(conn_id: str) -> None:
    with get_postgres_conn(conn_id).connect() as conn:
        execute_sql_file(conn, '/usr/local/airflow/dags/sql/task_one_query.sql')
