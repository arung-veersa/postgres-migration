import psycopg2
import time

# PostgreSQL connection details
PG_HOST = "awspgsqlpoc.cluster-c2an0zoe7aox.us-east-1.rds.amazonaws.com"
PG_PORT = 5432
PG_DATABASE = "conflict_management"
PG_USER = "cm_user"
PG_PASSWORD = "uGStSD2hgf&l16$1Bh"
PG_SCHEMA = "conflict"

# --- Configurable Durations ---
# NOTE: These durations are configured to work around a 30-minute (1800-second)
# database inactivity timeout. The query interval is set to be safely below this
# threshold.

# How long to keep the connection open to prevent timeouts (in seconds).
CONNECTION_DURATION = 3600  # 1 hour

# How often to run a keep-alive query while connected (in seconds).
QUERY_INTERVAL = 900  # 15 minutes

# How long to wait after closing the connection before reopening it (in seconds).
WAIT_INTERVAL = 60  # 1 minute

# SQL query to execute for keep-alive.
SQL_QUERY = 'select "ID" from settings limit 1'

def keep_alive():
    """
    Connects to the PostgreSQL database, holds the connection open for a
    configurable duration, and then disconnects. While connected, it runs
    a simple query periodically to keep the connection active.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            options=f'-c search_path={PG_SCHEMA}'
        )
        print(f"Successfully connected to PostgreSQL. Keeping connection open for {CONNECTION_DURATION} seconds.")
        
        cursor = conn.cursor()
        end_time = time.time() + CONNECTION_DURATION

        while time.time() < end_time:
            cursor.execute(SQL_QUERY)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Executed keep-alive query.")
            
            remaining_time = end_time - time.time()
            if remaining_time <= 0:
                break
            
            sleep_duration = min(QUERY_INTERVAL, remaining_time)
            time.sleep(sleep_duration)
        
        cursor.close()
        print("Finished keep-alive query loop.")

    except (Exception, psycopg2.Error) as error:
        print("Error during keep-alive:", error)
    
    finally:
        if conn:
            conn.close()
            print("Connection closed.")

if __name__ == "__main__":
    while True:
        keep_alive()
        wait_minutes = WAIT_INTERVAL / 60
        print(f"Waiting for {wait_minutes:.1f} min ({WAIT_INTERVAL} seconds) before the next keep-alive cycle.")
        time.sleep(WAIT_INTERVAL)
