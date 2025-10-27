import psycopg2
import time

# PostgreSQL connection details
PG_HOST = "awspgsqlpoc.cluster-c2an0zoe7aox.us-east-1.rds.amazonaws.com"
PG_PORT = 5432
PG_DATABASE = "conflict_management"
PG_USER = "cm_user"
PG_PASSWORD = "uGStSD2hgf&l16$1Bh"

def keep_alive():
    """
    Connects to the PostgreSQL database to keep the connection alive and then disconnects.
    """
    try:
        conn = psycopg2.connect(
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        print("Successfully connected to PostgreSQL to keep the connection alive.")
        conn.close()
        print("Connection closed.")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)

if __name__ == "__main__":
    while True:
        keep_alive()
        print("Waiting for 60 seconds before the next keep-alive.")
        time.sleep(60)
