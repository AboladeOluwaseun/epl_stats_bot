import os
import psycopg2

try:
    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'postgres'),
        database=os.environ.get('POSTGRES_DB', 'epl_stats'),
        user=os.environ.get('POSTGRES_USER', 'postgres'),
        password=os.environ.get('POSTGRES_PASSWORD', 'postgres')
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'dim_players'
        ORDER BY ordinal_position
    """)
    print("dim_players columns:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")
    
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'fact_player_stats'
        ORDER BY ordinal_position
    """)
    print("\nfact_player_stats columns:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
