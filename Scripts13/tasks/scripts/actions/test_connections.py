"""
Test Connections Action

Tests connectivity to Snowflake and PostgreSQL databases.
Available as a standalone action via ACTION=test_connections.
Also called internally by preflight.
"""

from config.settings import Settings
from lib.connections import ConnectionFactory
from lib.utils import get_logger

logger = get_logger(__name__)


def run_test_connections(settings: Settings) -> dict:
    """Test Snowflake and PostgreSQL connectivity."""
    sf_config = settings.get_snowflake_config()
    pg_config = settings.get_postgres_config()
    db_names = settings.get_database_names()

    conn_factory = ConnectionFactory(sf_config, pg_config)

    try:
        # Snowflake
        sf_manager = conn_factory.get_snowflake_manager()
        sf_conn = sf_manager.get_connection()
        cursor = sf_conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        sf_version = cursor.fetchone()[0]
        cursor.close()
        logger.info(f"Snowflake connection successful: {sf_version}")

        # PostgreSQL
        pg_manager = conn_factory.get_postgres_manager()
        pg_conn = pg_manager.get_connection(db_names['pg_database'])
        cursor = pg_conn.cursor()
        cursor.execute("SELECT version()")
        pg_version = cursor.fetchone()[0]
        cursor.close()
        pg_conn.close()
        logger.info("PostgreSQL connection successful")

        # Reference tables
        logger.info("Testing reference table access...")
        pg_conn = pg_manager.get_connection(db_names['pg_database'])
        cursor = pg_conn.cursor()

        list_tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        cursor.execute(list_tables_query, (db_names['pg_schema'],))
        available_tables = [row[0] for row in cursor.fetchall()]
        logger.info(
            f"Available tables in schema '{db_names['pg_schema']}': "
            f"{', '.join(available_tables) if available_tables else 'None'}"
        )

        reference_tables = {}
        test_tables = {
            'SETTINGS': 'settings',
            'EXCLUDED_AGENCY': 'excluded_agency',
            'EXCLUDED_SSN': 'excluded_ssn',
            'MPH': 'mph',
            'CONFLICTVISITMAPS': 'conflictvisitmaps',
        }

        for display_name, table_name in test_tables.items():
            if table_name in available_tables:
                try:
                    count_query = f'SELECT COUNT(*) FROM {db_names["pg_schema"]}.{table_name}'
                    cursor.execute(count_query)
                    count = cursor.fetchone()[0]
                    reference_tables[display_name] = {'name': table_name, 'count': count}
                    logger.info(f"  {display_name} table accessible as '{table_name}': {count} row(s)")
                except Exception as table_err:
                    logger.warning(f"  Found {table_name} but couldn't query it: {table_err}")
            else:
                logger.warning(f"  {display_name} table (expected as '{table_name}') not found")

        cursor.close()
        pg_conn.close()

        return {
            'status': 'success',
            'message': 'All connections tested successfully',
            'snowflake': {
                'connected': True,
                'version': sf_version,
                'database': db_names['sf_database'],
                'schema': db_names['sf_schema'],
            },
            'postgres': {
                'connected': True,
                'version': pg_version[:80],
                'database': db_names['pg_database'],
                'schema': db_names['pg_schema'],
                'available_tables': available_tables,
            },
            'reference_tables': reference_tables,
        }

    finally:
        conn_factory.close_all()
