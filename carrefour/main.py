import logging
import click

import sys
from os.path import abspath, dirname

# Add the parent directory to the sys.path
sys.path.append(dirname(dirname(abspath(__file__))))

from utils.postgres import close_pool
from load import (
    load_raw_data_to_postgres,
    load_staging_data_from_raw,
    load_prod_data_from_staging,
    create_staging_supermarkets_table,
    create_prod_supermarkets_table,
    load_products_raw_data_to_postgres,
    create_staging_products_table,
    create_prod_products_table,
    load_staging_products_from_raw,
    load_prod_products_from_staging,
)
from utils.logger import configure_logging
from extract import extract_products_from_html_files, extract_supermarkets


@click.group()
@click.pass_context
def cli(ctx):
    """Carrefour CLI Tool"""
    ctx.ensure_object(dict)


@cli.group()
@click.option("--debug", is_flag=True, help="Enable debug mode.")
@click.pass_context
def supermarket(ctx, debug):
    """Supermarket data pipeline commands"""
    ctx.ensure_object(dict)
    configure_logging(debug)


@supermarket.command()
@click.pass_context
def extract_raw(ctx):
    """Extract raw data from XML endpoint to PostgreSQL"""
    try:
        logging.info("Extracting raw supermarkets data from Carrefour XML endpoint")
        df = extract_supermarkets()

        logging.info(f"Found {len(df)} raw supermarkets records to load...")

        # Load raw data to PostgreSQL with date-based table naming
        load_raw_data_to_postgres(df)

    except Exception as e:
        logging.error(f"Error loading raw data: {e}")
        raise
    finally:
        close_pool()


@supermarket.command()
@click.pass_context
def transform_staging(ctx):
    """Transform raw data to staging schema"""
    try:
        logging.info("Transforming raw data to staging schema")

        # Get the most recent raw table
        from datetime import datetime

        current_date = datetime.now().strftime("%Y%m%d")
        raw_table_name = f"supermarket_{current_date}"

        # Consolidate data to staging
        load_staging_data_from_raw(raw_table_name)

        logging.info("Data transformed to staging successfully")

    except Exception as e:
        logging.error(f"Error transforming to staging: {e}")
        raise
    finally:
        close_pool()


@supermarket.command()
@click.pass_context
def deploy_prod(ctx):
    """Deploy staging data to production"""
    try:
        logging.info("Deploying staging data to production")

        # Promote data to production
        load_prod_data_from_staging()

        logging.info("Data deployed to production successfully")

    except Exception as e:
        logging.error(f"Error deploying to production: {e}")
        raise
    finally:
        close_pool()


@supermarket.command()
@click.pass_context
def run_pipeline(ctx):
    """Execute complete data pipeline"""
    try:
        logging.info("Starting full data pipeline execution")

        # Step 1: Extract and load raw data
        logging.info("Step 1: Extracting and loading raw data")
        df = extract_supermarkets()
        logging.info(f"Found {len(df)} raw supermarkets records to load...")

        # Generate table name for raw data
        from datetime import datetime

        current_date = datetime.now().strftime("%Y%m%d")
        raw_table_name = f"supermarket_{current_date}"

        # Load raw data to PostgreSQL
        load_raw_data_to_postgres(df)

        # Step 2: Create schemas if they don't exist
        logging.info("Step 2: Creating schemas and table structures")
        create_staging_supermarkets_table()
        create_prod_supermarkets_table()

        # Step 3: Consolidate to staging
        logging.info("Step 3: Transforming data to staging")
        load_staging_data_from_raw(raw_table_name)

        # Step 4: Promote to production
        logging.info("Step 4: Deploying data to production")
        load_prod_data_from_staging()

        logging.info("Full data pipeline completed successfully!")

    except Exception as e:
        logging.error(f"Error in full data pipeline: {e}")
        raise
    finally:
        close_pool()


@cli.group()
@click.option("--debug", is_flag=True, help="Enable debug mode.")
@click.pass_context
def products(ctx, debug):
    """Products data pipeline commands"""
    ctx.ensure_object(dict)
    configure_logging(debug)


@products.command()
@click.pass_context
def get_products(ctx):
    """Extract products from Carrefour website"""
    logging.info("Extracting products from Carrefour website")
    products = extract_products_from_html_files()
    logging.info(f"Found {products.height} products")
    logging.info(products)


@products.command()
@click.pass_context
def run_products_pipeline(ctx):
    """Execute complete products data pipeline"""
    logging.info("Starting full products data pipeline execution")
    products = extract_products_from_html_files()
    logging.info(f"Found {products.height} products")

    # Generate table name for raw data
    from datetime import datetime

    current_date = datetime.now().strftime("%Y%m%d")
    raw_table_name = f"products_{current_date}"

    logging.info(products)

    # Load raw data to PostgreSQL
    load_products_raw_data_to_postgres(products)

    # Create schemas and table structures
    logging.info("Creating schemas and table structures for products")
    create_staging_products_table()
    create_prod_products_table()

    # Transform to staging
    logging.info("Transforming products data to staging")
    load_staging_products_from_raw(raw_table_name)

    # Promote to production
    logging.info("Promoting products data to production")
    load_prod_products_from_staging()

    logging.info("Full products data pipeline completed successfully!")


if __name__ == "__main__":
    cli()
