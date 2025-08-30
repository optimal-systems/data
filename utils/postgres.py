# pylint: disable=C0114
import os
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

# Load environment variables from .env file
load_dotenv()

# pylint: disable=W0603
_POSTGRES_POOL = None


def get_postgres_config():
    """
    Get PostgreSQL configuration from environment variables with defaults.

    Returns:
        dict: Configuration dictionary for PostgreSQL connection
    """
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "optimal"),
        "user": os.getenv("POSTGRES_USER", "data_ingestor"),
        "password": os.getenv("POSTGRES_PASSWORD", "di_supersecret"),
        "minconn": int(os.getenv("POSTGRES_MIN_CONN", "1")),
        "maxconn": int(os.getenv("POSTGRES_MAX_CONN", "10")),
    }


def get_postgres_pool():
    """
    Get or create a PostgreSQL connection pool.

    Returns:
        SimpleConnectionPool: PostgreSQL connection pool
    """
    global _POSTGRES_POOL
    if _POSTGRES_POOL is None:
        config = get_postgres_config()
        _POSTGRES_POOL = SimpleConnectionPool(
            minconn=config["minconn"],
            maxconn=config["maxconn"],
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
        )
    return _POSTGRES_POOL


def get_postgres_connection():
    """
    Get a connection from the PostgreSQL pool.

    Returns:
        PostgreSQL connection object
    """
    pool = get_postgres_pool()
    return pool.getconn()


def return_postgres_connection(conn):
    """
    Return a connection to the PostgreSQL pool.

    Parameters:
        conn: PostgreSQL connection object to return to the pool
    """
    pool = get_postgres_pool()
    pool.putconn(conn)


def execute_query(query, params=None, fetch=True):
    """
    Execute a SQL query with proper connection management.

    Parameters:
        query: SQL query to execute
        params: Query parameters
        fetch: Whether to fetch results (True for SELECT, False for INSERT/UPDATE/DELETE)

    Returns:
        Query results if fetch=True, None otherwise
    """
    conn = None
    try:
        conn = get_postgres_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)

            if fetch:
                # Check if there are results to fetch (e.g., for SELECT statements)
                if cursor.description:
                    result = cursor.fetchall()
                    return [dict(row) for row in result]
                return None  # No results to fetch (e.g., for DDL/DML statements without RETURNING)
            else:
                conn.commit()
                return None

    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            return_postgres_connection(conn)


def test_connection():
    """
    Test PostgreSQL connection.

    Returns:
        True if connection successful, False otherwise
    """
    try:
        conn = get_postgres_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            return_postgres_connection(conn)
            return result[0] == 1
    except Exception:
        return False


def close_pool():
    """
    Close the PostgreSQL connection pool.
    """
    global _POSTGRES_POOL
    if _POSTGRES_POOL:
        _POSTGRES_POOL.closeall()
        _POSTGRES_POOL = None
