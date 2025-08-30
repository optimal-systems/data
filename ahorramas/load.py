# Add the parent directory to the sys.path
import sys
from os.path import dirname, abspath
import os
import logging
import json
import polars as pl
from datetime import datetime
from utils.postgres import execute_query


sys.path.append(dirname(dirname(abspath(__file__))))


def load_data(data: pl.DataFrame, file_name: str) -> None:
    """
    Load Ahorramas data on a JSON file

    Parameters:
    - data (pl.DataFrame): Ahorramas data.
    - file_name (str): Name of the JSON file.

    Returns:
    - None
    """
    output_directory = dirname(dirname(abspath(__file__))) + "/ahorramas/data"
    os.makedirs(output_directory, exist_ok=True)
    output_path = os.path.join(output_directory, file_name)
    logging.info("Saving Ahorramas supermarkets metadata into file: %s", file_name)
    with open(output_path, "w", encoding="utf-8") as file:
        data.write_json(file)
    logging.info("Ahorramas supermarkets metadata saved into file: %s", file_name)


def load_raw_data_to_postgres(data: pl.DataFrame) -> str:
    """
    Load raw supermarkets data to PostgreSQL with date-based table naming.

    Creates a table with format: raw_supermarket_YYYYMMDD

    Parameters:
    - data (pl.DataFrame): Raw supermarkets data from API

    Returns:
    - str: Name of the created table
    """
    # Generate table name with current date
    current_date = datetime.now().strftime("%Y%m%d")
    table_name = f"raw_supermarket_{current_date}"

    logging.info("Creating raw data table: %s", table_name)

    # Create table for raw data
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        raw_data JSONB NOT NULL,
        extracted_at TIMESTAMPTZ DEFAULT NOW(),
        processed BOOLEAN DEFAULT FALSE
    );
    """

    # Create index on raw_data for JSON queries
    create_index_query = f"""
    CREATE INDEX IF NOT EXISTS idx_{table_name}_raw_data ON {table_name} USING GIN (raw_data);
    """

    try:
        # Create table and index
        execute_query(create_table_query, fetch=False)
        execute_query(create_index_query, fetch=False)

        logging.info("Raw data table and index created successfully")

        # Insert raw data as JSON
        for row in data.iter_rows(named=True):
            insert_query = f"""
            INSERT INTO {table_name} (raw_data) 
            VALUES (%s);
            """

            # Convert row to JSON string
            raw_json = json.dumps(dict(row))
            execute_query(insert_query, (raw_json,), fetch=False)

        logging.info(
            "Raw data loaded to PostgreSQL successfully in table: %s", table_name
        )
        return table_name

    except Exception as e:
        logging.error("Error loading raw data to PostgreSQL: %s", e)
        raise
