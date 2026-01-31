import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
import time

DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASSWORD = "password"
TARGET_DB = "idem_test"

def init_db():
    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}...")
    
    # Connect to 'postgres' db to create the target db
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
    except Exception as e:
        print(f"Error connecting to Postgres: {e}")
        print("Tip: Ensure docker container is running (docker-compose up -d)")
        sys.exit(1)

    # Check if DB exists
    cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{TARGET_DB}'")
    exists = cur.fetchone()

    if not exists:
        print(f"Database '{TARGET_DB}' does not exist. Creating...")
        cur.execute(f"CREATE DATABASE {TARGET_DB}")
        print(f"Database '{TARGET_DB}' created successfully.")
    else:
        print(f"Database '{TARGET_DB}' already exists.")

    cur.close()
    conn.close()

    # Now connect to the new DB to apply schema (Optional: usually app does this, but we can do basics here)
    # For this project, we will rely on manual or app-driven migration application, 
    # but this script ensures the DB is ready for connection.
    print("Done.")

if __name__ == "__main__":
    try:
        init_db()
    except ImportError:
        print("Error: 'psycopg2' module not found.")
        print("Please install it using: pip install psycopg2-binary")
