import sys
import time
import logging
from os.path import dirname, abspath
import requests
import polars as pl

# Add the parent directory to the sys.path
sys.path.append(dirname(dirname(abspath(__file__))))


def fetch_supermarkets():
    """
    Fetch supermarkets from Ahorramas API
    """
    url = "https://www.ahorramas.com/on/demandware.store/Sites-Ahorramas-Site/es/Stores-FindStore"
    params = {
        "update": "true",
        "showMap": "false",
        "postalCodeInput": "España",
    }
    session = requests.Session()
    retries = 5
    timeout = 5
    delay = 5

    for _ in range(0, retries):
        try:
            logging.info("Fetching supermarkets data from Ahorramas API")
            response = session.get(url, headers={}, params=params, timeout=timeout)
            session.close()
            logging.info("Supermarkets data fetched from Ahorramas API")
            return response.json()

        except requests.Timeout as exception:
            logging.error("Timeout error: %s", exception)
            time.sleep(delay)

        except requests.RequestException as exception:
            logging.warning("Request error: %s", exception)
            time.sleep(delay)

    logging.error(
        "Max retries reached. Failed to fetch supermarkets data from Ahorramas API"
    )
    raise RuntimeError("Failed to fetch supermarkets from Ahorramas API")


def extract_supermarkets() -> pl.DataFrame:
    """
    Transform the supermarkets data from Ahorramas API into a polars DataFrame

    Parameters:
        df: pl.DataFrame: The supermarkets data from Ahorramas API

    Returns:
        pl.DataFrame: The transformed supermarkets data
    """
    raw_data = fetch_supermarkets()
    raw_supermarkets = raw_data["stores"]

    items = []

    for supermarket in raw_supermarkets:
        items.append(
            {
                "store_id": supermarket["codtda"],
                "address": supermarket["direccion"],
                "schedule": supermarket["horario"],
                "holidays": supermarket["festivos"],
                "latitude": supermarket["latitude"],
                "longitude": supermarket["longitude"],
            }
        )

    return pl.DataFrame(items)
