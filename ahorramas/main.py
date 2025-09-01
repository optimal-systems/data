import logging
import click

import sys
from os.path import abspath, dirname
import polars as pl

# Add the parent directory to the sys.path
sys.path.append(dirname(dirname(abspath(__file__))))

from utils.postgres import close_pool, configure_products_search
from load import (
    load_raw_data_to_postgres,
    load_staging_data_from_raw,
    load_prod_data_from_staging,
    create_staging_supermarkets_table,
    create_prod_supermarkets_table,
    load_raw_products_to_postgres,
    load_staging_products_from_raw,
    load_prod_products_from_staging,
    create_staging_products_table,
    create_prod_products_table,
)
from utils.logger import configure_logging
from extract import (
    extract_products,
    extract_supermarkets,
    extract_categories,
    extract_category_slugs,
)


@click.group()
@click.pass_context
def cli(ctx):
    """Ahorramas CLI Tool"""
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
    """Extract raw data from API to PostgreSQL"""
    try:
        logging.info("Extracting raw supermarkets data from API")
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
def get_categories(ctx):
    """Extract categories from Ahorramas website"""
    try:
        logging.info("Extracting categories from Ahorramas website")

        # Extract categories
        categories = extract_categories()

        # Log the categories
        logging.info(f"Found {len(categories)} categories")
        logging.info(f"Categories: {categories}")

        logging.info("Categories extracted and saved successfully")

    except Exception as e:
        logging.error(f"Error extracting categories: {e}")
        raise
    finally:
        close_pool()


@products.command()
@click.pass_context
def get_category_slugs(ctx):
    """Extract category slugs from Ahorramas website"""
    logging.info("Extracting category slugs from Ahorramas website")
    category_slugs = extract_category_slugs()
    logging.info(f"Found {len(category_slugs)} category slugs")
    logging.info(category_slugs)


@products.command()
@click.pass_context
def get_products(ctx):
    """Extract products from all categories and save to PostgreSQL"""
    try:
        logging.info("Extracting products from all categories")
        categories = extract_category_slugs()

        # Extract products for each category and merge them into a single dataframe
        df = pl.concat(
            [
                extract_products(f"https://www.ahorramas.com/{category}/")
                for category in categories
            ]
        )
        logging.info(f"Found {len(df)} products")

        # Load raw products data to PostgreSQL
        logging.info("Loading raw products data to PostgreSQL")
        load_raw_products_to_postgres(df)

        # Create schemas and table structures
        logging.info("Creating schemas and table structures for products")
        create_staging_products_table()
        create_prod_products_table()

        # Get the most recent raw table name
        from datetime import datetime

        current_date = datetime.now().strftime("%Y%m%d")
        raw_table_name = f"products_{current_date}"

        # Transform to staging
        logging.info("Transforming products data to staging")
        load_staging_products_from_raw(raw_table_name)

        # Promote to production
        logging.info("Promoting products data to production")
        load_prod_products_from_staging()

        logging.info("Products pipeline completed successfully!")

    except Exception as e:
        logging.error(f"Error in products pipeline: {e}")
        raise
    finally:
        close_pool()


@products.command()
@click.pass_context
def extract_raw_products(ctx):
    """Extract raw products data from all categories to PostgreSQL"""
    try:
        logging.info("Extracting raw products data from all categories")
        categories = extract_category_slugs()

        # Extract products for each category and merge them into a single dataframe
        df = pl.concat(
            [
                extract_products(f"https://www.ahorramas.com/{category}/")
                for category in categories
            ]
        )
        logging.info(f"Found {len(df)} products")

        # Load raw products data to PostgreSQL
        load_raw_products_to_postgres(df)

    except Exception as e:
        logging.error(f"Error extracting raw products data: {e}")
        raise
    finally:
        close_pool()


@products.command()
@click.pass_context
def transform_staging_products(ctx):
    """Transform raw products data to staging schema"""
    try:
        logging.info("Transforming raw products data to staging schema")

        # Get the most recent raw table
        from datetime import datetime

        current_date = datetime.now().strftime("%Y%m%d")
        raw_table_name = f"products_{current_date}"

        # Consolidate data to staging
        load_staging_products_from_raw(raw_table_name)

        logging.info("Products data transformed to staging successfully")

    except Exception as e:
        logging.error(f"Error transforming products to staging: {e}")
        raise
    finally:
        close_pool()


@products.command()
@click.pass_context
def deploy_prod_products(ctx):
    """Deploy staging products data to production"""
    try:
        logging.info("Deploying staging products data to production")

        # Promote data to production
        load_prod_products_from_staging()

        logging.info("Products data deployed to production successfully")

    except Exception as e:
        logging.error(f"Error deploying products to production: {e}")
        raise
    finally:
        close_pool()


@products.command()
@click.pass_context
def run_products_pipeline(ctx):
    """Execute complete products data pipeline"""
    try:
        logging.info("Starting full products data pipeline execution")

        # Step 1: Extract and load raw data
        logging.info("Step 1: Extracting and loading raw products data")
        categories = extract_category_slugs()
        df = pl.concat(
            [
                extract_products(f"https://www.ahorramas.com/{category}/")
                for category in categories
            ]
        )
        logging.info(f"Found {len(df)} products")

        # Generate table name for raw data
        from datetime import datetime

        current_date = datetime.now().strftime("%Y%m%d")
        raw_table_name = f"products_{current_date}"

        # Load raw data to PostgreSQL
        load_raw_products_to_postgres(df)

        # Step 2: Create schemas if they don't exist
        logging.info("Step 2: Creating schemas and table structures")
        create_staging_products_table()
        create_prod_products_table()

        # Step 3: Consolidate to staging
        logging.info("Step 3: Transforming products data to staging")
        load_staging_products_from_raw(raw_table_name)

        # Step 4: Promote to production
        logging.info("Step 4: Deploying products data to production")
        load_prod_products_from_staging()

        # Step 5: Configure search functionality
        logging.info("Step 5: Configuring products search functionality")
        configure_products_search()

        logging.info("Full products data pipeline completed successfully!")

    except Exception as e:
        logging.error(f"Error in full products data pipeline: {e}")
        raise
    finally:
        close_pool()


if __name__ == "__main__":
    cli()
