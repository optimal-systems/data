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


def configure_products_search():
    """
    Configure full-text search functionality for products table.
    This function sets up the search configuration, creates the search column,
    and creates the search function for products.
    """
    search_config_sql = """
    -- 0) Limpieza: borra la función antigua (5 args) si está por ahí
    DO $$
    BEGIN
      IF to_regprocedure('prod.search_products(text,integer,integer,boolean,boolean)') IS NOT NULL THEN
        DROP FUNCTION prod.search_products(text,integer,integer,boolean,boolean);
      END IF;
    END$$;

    -- 1) Config mínima por si no está (idempotente)
    CREATE SCHEMA IF NOT EXISTS prod;
    CREATE EXTENSION IF NOT EXISTS unaccent;

    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_ts_config c
        JOIN pg_namespace n ON n.oid = c.cfgnamespace
        WHERE n.nspname = 'prod' AND c.cfgname = 'es_unaccent'
      ) THEN
        CREATE TEXT SEARCH CONFIGURATION prod.es_unaccent (COPY = spanish);
        ALTER TEXT SEARCH CONFIGURATION prod.es_unaccent
          ALTER MAPPING FOR hword, hword_part, word WITH unaccent, spanish_stem;
      END IF;
    END$$;

    -- 2) Deja la columna 'search' EXACTAMENTE con name+supermarket
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='prod' AND table_name='products' AND column_name='search'
      ) THEN
        ALTER TABLE prod.products DROP COLUMN search;
      END IF;
    END$$;

    ALTER TABLE prod.products
    ADD COLUMN search tsvector
    GENERATED ALWAYS AS (
      setweight(to_tsvector('simple',           COALESCE(supermarket,'')), 'A') ||
      setweight(to_tsvector('prod.es_unaccent', COALESCE(name,'')),        'B')
    ) STORED;

    -- 3) Índice GIN sobre la columna search
    CREATE INDEX IF NOT EXISTS idx_prod_products_search
    ON prod.products USING GIN(search);

    -- 4) Función definitiva (1 parámetro), SIN "último snapshot", con is_active = TRUE
    CREATE OR REPLACE FUNCTION prod.search_products(term text) 
    RETURNS TABLE(
      id int,
      name text,
      supermarket text,
      price numeric,
      url text,
      rank real
    )
    LANGUAGE SQL
    STABLE
    PARALLEL SAFE
    AS
    $$
      SELECT id, name, supermarket, price, url,
             -- mismo patrón que tu referencia: dos señales
             ts_rank(search, websearch_to_tsquery('prod.es_unaccent', term)) +
             ts_rank(search, websearch_to_tsquery('simple', term)) AS rank
      FROM prod.products
      WHERE is_active
        AND (
             search @@ websearch_to_tsquery('prod.es_unaccent', term)
          OR search @@ websearch_to_tsquery('simple', term)
        )
      ORDER BY rank DESC
    $$;
    """

    execute_query(search_config_sql, fetch=False)


def close_pool():
    """
    Close the PostgreSQL connection pool.
    """
    global _POSTGRES_POOL
    if _POSTGRES_POOL:
        _POSTGRES_POOL.closeall()
        _POSTGRES_POOL = None
