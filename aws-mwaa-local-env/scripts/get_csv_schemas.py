import pandas as pd

from scripts.get_postgres_conn import get_postgres_conn

def get_csv_schemas(conn_id: str) -> pd.DataFrame:
    with open('/usr/local/airflow/dags/sql/get_csv_schemas_table.sql', 'r') as f:
        sql_query = f.read()
    with get_postgres_conn(conn_id).connect() as conn:
        return pd.read_sql_query(sql_query, conn)
