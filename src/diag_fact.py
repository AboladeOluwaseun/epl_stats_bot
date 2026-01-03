import psycopg2
import os

def check_fact_schema():
    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'postgres'),
        port=os.environ.get('POSTGRES_PORT', 5432),
        database=os.environ.get('POSTGRES_DB', 'epl_stats'),
        user=os.environ.get('POSTGRES_USER', 'postgres'),
        password=os.environ.get('POSTGRES_PASSWORD', 'postgres')
    )
    cur = conn.cursor()
    
    print("\nSchema for fact_player_stats:")
    cur.execute(f"""
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns 
        WHERE table_name = 'fact_player_stats'
        ORDER BY ordinal_position
    """)
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} {f'({row[2]})' if row[2] else ''}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_fact_schema()
