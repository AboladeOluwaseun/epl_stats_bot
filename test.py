# test_db_connection.py
import psycopg2

print("🔌 Testing direct database connection...")

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="epl_stats",
        user="postgres",
        password="admin"
    )
    print("✅ Connection successful!")
    
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print(f"📊 PostgreSQL version: {version[0][:50]}...")
    
    cur.close()
    conn.close()
    
except psycopg2.OperationalError as e:
    print(f"❌ Connection failed: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")