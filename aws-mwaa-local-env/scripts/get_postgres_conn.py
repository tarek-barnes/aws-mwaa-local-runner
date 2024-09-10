from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine

def get_postgres_conn(conn_id):
    conn = BaseHook.get_connection(conn_id)
    port = conn.port or 5432
    connection_string = (
        f"postgresql+psycopg2://"
        f"{conn.login}:{conn.password}@"
        f"{conn.host}:{port}/"
        f"{conn.schema}"
    )
    return create_engine(connection_string)
