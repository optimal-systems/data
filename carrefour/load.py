# Add the parent directory to the sys.path
import sys
from os.path import dirname, abspath
import os
import logging
import polars as pl
from datetime import datetime
from utils.postgres import execute_query


sys.path.append(dirname(dirname(abspath(__file__))))


def load_data(data: pl.DataFrame, file_name: str) -> None:
    """
    Load Carrefour data on a JSON file

    Parameters:
    - data (pl.DataFrame): Carrefour data.
    - file_name (str): Name of the JSON file.

    Returns:
    - None
    """
    output_directory = dirname(dirname(abspath(__file__))) + "/carrefour/data"
    os.makedirs(output_directory, exist_ok=True)
    output_path = os.path.join(output_directory, file_name)
    logging.info("Saving Carrefour supermarkets metadata into file: %s", file_name)
    with open(output_path, "w", encoding="utf-8") as file:
        data.write_json(file)
    logging.info("Carrefour supermarkets metadata saved into file: %s", file_name)


def load_raw_data_to_postgres(data: pl.DataFrame) -> None:
    """
    Load raw supermarkets data to PostgreSQL with date-based table naming.

    Creates a table with format: raw.supermarket_YYYYMMDD
    Each DataFrame column becomes a text column in PostgreSQL.
    If table exists, it will be replaced to avoid duplicates.

    Parameters:
    - data (pl.DataFrame): Raw supermarkets data from API

    Returns:
    - None
    """
    # Generate table name with current date
    current_date = datetime.now().strftime("%Y%m%d")
    table_name = f"supermarket_{current_date}"
    full_table_name = f"raw.{table_name}"

    logging.info("Creating raw data table: %s", full_table_name)

    # Get DataFrame columns and create table columns
    columns = data.columns
    column_definitions = [f'"{col}" TEXT' for col in columns]

    # Drop table if exists and recreate to avoid duplicates
    drop_table_query = f"DROP TABLE IF EXISTS {full_table_name};"
    create_table_query = f"""
    CREATE TABLE {full_table_name} (
        {", ".join(column_definitions)}
    );
    """

    try:
        # Drop existing table and recreate
        execute_query(drop_table_query, fetch=False)
        execute_query(create_table_query, fetch=False)

        logging.info("Raw data table created successfully")

        # Insert data row by row
        for row in data.iter_rows(named=True):
            # Create placeholders for each column
            placeholders = ", ".join(["%s"] * len(columns))
            column_names = ", ".join([f'"{col}"' for col in columns])

            insert_query = f"""
            INSERT INTO {full_table_name} ({column_names}) 
            VALUES ({placeholders});
            """

            # Get values in the same order as columns
            values = [
                str(row[col]) if row[col] is not None else None for col in columns
            ]
            execute_query(insert_query, values, fetch=False)

        logging.info(
            "Raw data loaded to PostgreSQL successfully in table: %s", full_table_name
        )

    except Exception as e:
        logging.error("Error loading raw data to PostgreSQL: %s", e)
        raise


def create_staging_supermarkets_table() -> None:
    """
    Create staging schema and base table structure for consolidated data.
    """
    logging.info("Creating staging supermarkets table")

    # Create staging table with well-defined columns and types based on raw data structure
    create_staging_table_query = """
    CREATE TABLE IF NOT EXISTS staging.supermarkets (
        id SERIAL,
        store_id VARCHAR(50),
        address TEXT,
        schedule TEXT,
        holidays TEXT,
        latitude DECIMAL(10, 8),
        longitude DECIMAL(11, 8),
        extracted_date DATE,
        name VARCHAR(50) NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    ) PARTITION BY RANGE (extracted_date);
    """

    execute_query(create_staging_table_query, fetch=False)

    # Create indexes for better performance
    create_indexes_query = """
    CREATE INDEX IF NOT EXISTS idx_staging_supermarkets_extracted_date ON staging.supermarkets(extracted_date);
    CREATE INDEX IF NOT EXISTS idx_staging_supermarkets_location ON staging.supermarkets(latitude, longitude);
    CREATE INDEX IF NOT EXISTS idx_staging_supermarkets_name ON staging.supermarkets(name);
    CREATE INDEX IF NOT EXISTS idx_staging_supermarkets_store_id ON staging.supermarkets(store_id);
    """

    execute_query(create_indexes_query, fetch=False)

    logging.info("Staging supermarkets table created successfully")

    try:
        execute_query(create_staging_table_query, fetch=False)
        execute_query(create_indexes_query, fetch=False)
        logging.info("Staging supermarkets table created successfully")
    except Exception as e:
        logging.error("Error creating staging table: %s", e)
        raise


def create_prod_supermarkets_table() -> None:
    """
    Create production schema and final table structure for API consumption.
    """
    logging.info("Creating production supermarkets table")

    # Create production table optimized for API consumption with partitioning
    create_prod_table_query = """
    CREATE TABLE IF NOT EXISTS prod.supermarkets (
        id SERIAL,
        store_id VARCHAR(50) NOT NULL,
        address TEXT NOT NULL,
        schedule TEXT,
        holidays TEXT,
        latitude DECIMAL(10, 8),
        longitude DECIMAL(11, 8),
        is_active BOOLEAN DEFAULT TRUE,
        name VARCHAR(50) NOT NULL,
        last_updated TIMESTAMPTZ DEFAULT NOW(),
        created_at TIMESTAMPTZ DEFAULT NOW(),
        extracted_date DATE
    ) PARTITION BY RANGE (extracted_date);
    """

    execute_query(create_prod_table_query, fetch=False)

    # Add unique constraint if it doesn't exist (based on store_id, date and name)
    add_unique_constraint_query = """
    DO $$ 
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint 
            WHERE conname = 'prod_supermarkets_store_id_extracted_date_key'
        ) THEN
            ALTER TABLE prod.supermarkets 
            ADD CONSTRAINT prod_supermarkets_store_id_extracted_date_key 
            UNIQUE (store_id, extracted_date, name);
        END IF;
    END $$;
    """

    execute_query(add_unique_constraint_query, fetch=False)

    # Create indexes for API performance
    create_prod_indexes_query = """
    CREATE INDEX IF NOT EXISTS idx_prod_supermarkets_location ON prod.supermarkets(latitude, longitude);
    CREATE INDEX IF NOT EXISTS idx_prod_supermarkets_active ON prod.supermarkets(is_active);
    CREATE INDEX IF NOT EXISTS idx_prod_supermarkets_extracted_date ON prod.supermarkets(extracted_date);
    CREATE INDEX IF NOT EXISTS idx_prod_supermarkets_name ON prod.supermarkets(name);
    CREATE INDEX IF NOT EXISTS idx_prod_supermarkets_store_id ON prod.supermarkets(store_id);
    """

    execute_query(create_prod_indexes_query, fetch=False)

    logging.info("Production schema and table structure created successfully")


def load_staging_data_from_raw(raw_table_name: str) -> None:
    """
    Load and consolidate data from raw schema to staging schema.

    Parameters:
    - raw_table_name (str): Name of the raw table to consolidate
    """
    logging.info(f"Loading staging data from raw.{raw_table_name}...")

    # Get the extracted date from table name
    extracted_date = raw_table_name.replace("supermarket_", "")

    # Create staging table if it doesn't exist
    create_staging_supermarkets_table()

    # Create partition for the specific date if it doesn't exist
    partition_name = f"supermarkets_{extracted_date}"
    create_partition_query = f"""
    CREATE TABLE IF NOT EXISTS staging.{partition_name} 
    PARTITION OF staging.supermarkets 
    FOR VALUES FROM ('{extracted_date[:4]}-{extracted_date[4:6]}-{extracted_date[6:8]}') 
    TO ('{extracted_date[:4]}-{extracted_date[4:6]}-{extracted_date[6:8]}'::date + INTERVAL '1 day');
    """

    execute_query(create_partition_query, fetch=False)

    # Delete existing data for this date to avoid duplicates
    delete_existing_query = f"""
    DELETE FROM staging.supermarkets 
    WHERE extracted_date = '{extracted_date[:4]}-{extracted_date[4:6]}-{extracted_date[6:8]}'::date
    AND name = 'carrefour';
    """

    execute_query(delete_existing_query, fetch=False)
    logging.info(
        f"Deleted existing staging data for date: {extracted_date[:4]}-{extracted_date[4:6]}-{extracted_date[6:8]}"
    )

    # Insert consolidated data into staging table
    insert_staging_query = f"""
    INSERT INTO staging.supermarkets (
        store_id, address, schedule, holidays, latitude, longitude, extracted_date, name
    )
    SELECT 
        COALESCE(store_id, '') as store_id,
        COALESCE(address, '') as address,
        COALESCE(schedule, '') as schedule,
        COALESCE(holidays, '') as holidays,
        CASE 
            WHEN latitude ~ '^[0-9.-]+$' THEN CAST(latitude AS DECIMAL(10, 8))
            ELSE NULL 
        END as latitude,
        CASE 
            WHEN longitude ~ '^[0-9.-]+$' THEN CAST(longitude AS DECIMAL(11, 8))
            ELSE NULL 
        END as longitude,
        '{extracted_date[:4]}-{extracted_date[4:6]}-{extracted_date[6:8]}'::date as extracted_date,
        'carrefour' as name
    FROM raw.{raw_table_name};
    """

    execute_query(insert_staging_query, fetch=False)

    logging.info(f"Staging data loaded from raw.{raw_table_name} successfully")


def load_prod_data_from_staging() -> None:
    """
    Load and promote data from staging schema to production schema.
    This function will upsert data, updating existing records and inserting new ones.
    """
    logging.info("Loading production data from staging...")

    # Create production table if it doesn't exist
    create_prod_supermarkets_table()

    # Get the most recent extracted date from staging
    get_latest_date_query = """
    SELECT MAX(extracted_date) as latest_date FROM staging.supermarkets WHERE name = 'carrefour';
    """

    latest_date_result = execute_query(get_latest_date_query)
    if not latest_date_result:
        logging.error("No data found in staging.supermarkets")
        return

    latest_date = latest_date_result[0]["latest_date"]

    # Create partition for the specific date if it doesn't exist
    partition_name = f"supermarkets_{latest_date.strftime('%Y%m%d')}"
    create_partition_query = f"""
    CREATE TABLE IF NOT EXISTS prod.{partition_name} 
    PARTITION OF prod.supermarkets 
    FOR VALUES FROM ('{latest_date}') 
    TO ('{latest_date}'::date + INTERVAL '1 day');
    """

    execute_query(create_partition_query, fetch=False)

    upsert_query = """
    INSERT INTO prod.supermarkets (
        store_id, address, schedule, holidays, latitude, longitude, extracted_date, name
    )
    SELECT DISTINCT ON (s.latitude, s.longitude, s.extracted_date, s.name)
        s.store_id, s.address, s.schedule, s.holidays, s.latitude, s.longitude, s.extracted_date, s.name
    FROM staging.supermarkets s
    WHERE s.extracted_date = %s AND s.name = 'carrefour'
    ORDER BY s.latitude, s.longitude, s.extracted_date, s.name,
             (s.store_id IS NULL), s.store_id DESC
    ON CONFLICT (latitude, longitude, extracted_date, name) DO UPDATE SET
        address = EXCLUDED.address,
        schedule = EXCLUDED.schedule,
        holidays = EXCLUDED.holidays,
        store_id = COALESCE(EXCLUDED.store_id, prod.supermarkets.store_id),
        last_updated = NOW();
    """

    execute_query(upsert_query, (latest_date,), fetch=False)

    logging.info("Production data loaded from staging successfully")
