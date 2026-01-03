import psycopg2
import os

def check_schema():
    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'postgres'),
        port=os.environ.get('POSTGRES_PORT', 5432),
        database=os.environ.get('POSTGRES_DB', 'epl_stats'),
        user=os.environ.get('POSTGRES_USER', 'postgres'),
        password=os.environ.get('POSTGRES_PASSWORD', 'postgres')
    )
    cur = conn.cursor()
    
    tables = ['dim_players', 'fact_player_stats']
    
    for table in tables:
        print(f"\nSchema for {table}:")
        cur.execute(f"""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns 
            WHERE table_name = '{table}'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        if not columns:
            print(f"  TABLE {table} NOT FOUND!")
        for row in columns:
            print(f"  {row[0]}: {row[1]} {f'({row[2]})' if row[2] else ''}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_schema()
