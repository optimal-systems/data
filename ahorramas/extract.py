import sys
import time
import logging
from os.path import dirname, abspath
import requests

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
            response = session.get(url, headers={}, params=params, timeout=timeout)
            session.close()
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
