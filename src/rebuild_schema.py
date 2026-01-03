import psycopg2
import os
from pathlib import Path

def rebuild_schema():
    # Path to the local DDL file
    ddl_path = Path('/opt/airflow/sql/ddl/create_tables.sql')
    if not ddl_path.exists():
        print(f"Error: {ddl_path} not found inside container.")
        return

    print(f"Reading DDL from {ddl_path}...")
    with open(ddl_path, 'r') as f:
        sql = f.read()

    print("Connecting to database...")
    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'postgres'),
        port=os.environ.get('POSTGRES_PORT', 5432),
        database=os.environ.get('POSTGRES_DB', 'epl_stats'),
        user=os.environ.get('POSTGRES_USER', 'postgres'),
        password=os.environ.get('POSTGRES_PASSWORD', 'postgres')
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("Executing DDL script (including DROPs)...")
    try:
        cur.execute(sql)
        print("Schema rebuilt successfully!")
    except Exception as e:
        print(f"Error executing DDL: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    rebuild_schema()
