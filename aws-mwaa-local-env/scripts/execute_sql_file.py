from scripts.get_postgres_conn import get_postgres_conn

def execute_sql_file(conn, filepath):
    with open(filepath, 'r') as f:
        sql_to_execute = f.read()

    conn.execute(sql_to_execute)
