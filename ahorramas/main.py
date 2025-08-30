import logging
import click

import sys
from os.path import abspath, dirname

# Add the parent directory to the sys.path
sys.path.append(dirname(dirname(abspath(__file__))))

from utils.postgres import test_connection, close_pool
from load import load_data, load_raw_data_to_postgres
from utils.logger import configure_logging
from extract import extract_supermarkets


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug mode.")
@click.pass_context
def cli(ctx, debug):
    """Ahorramas CLI Tool"""
    ctx.ensure_object(dict)
    configure_logging(debug)


@cli.command()
@click.option(
    "--output_file",
    type=str,
    default="supermarkets.json",
    help="E.g: supermarkets.json",
)
@click.pass_context
def get_supermarkets(ctx, output_file):
    """Extract and load supermarkets data from Ahorramas API to a JSON file"""
    df = extract_supermarkets()
    load_data(df, output_file)


@cli.command()
@click.pass_context
def test_postgres_connection(ctx):
    """Test PostgreSQL connection"""
    try:
        connection_status = test_connection()
        if connection_status:
            logging.info("PostgreSQL connection successful")
        else:
            logging.error("PostgreSQL connection failed")
    finally:
        close_pool()


@cli.command()
@click.pass_context
def load_raw_supermarkets_to_postgres(ctx):
    """Load raw supermarkets data from Ahorramas API to PostgreSQL with date-based table naming"""
    try:
        logging.info("Extracting raw supermarkets data from API...")
        df = extract_supermarkets()

        logging.info(f"Found {len(df)} raw supermarkets records to load...")

        # Load raw data to PostgreSQL with date-based table naming
        table_name = load_raw_data_to_postgres(df)

    except Exception as e:
        logging.error(f"Error loading raw data: {e}")
        raise
    finally:
        close_pool()


if __name__ == "__main__":
    cli()
