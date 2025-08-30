import sys
import time
import logging
from os.path import dirname, abspath
import requests
import polars as pl
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse

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


def fetch_html_content(url, params=None, retries=5, timeout=5, delay=5):
    """
    Fetches HTML content from the specified URL using the requests library.
    If the request fails due to a timeout or other request exception,
    it will retry the specified number of times.

    Parameters:
    url (str): The URL to fetch the HTML content from.
    params (dict, optional): Additional parameters to be sent with the request. Defaults to None.
    retries (int, optional): The number of times to retry the request in case of failure.
                             Defaults to 5.
    timeout (int, optional): The timeout for the request in seconds. Defaults to 5.
    delay (int, optional): The delay between retries in seconds. Defaults to 5.

    Returns:
    str: The prettified HTML content of the fetched page.

    Raises:
    RuntimeError: If the maximum number of retries is reached without successful response.
    """
    if params is None:
        params = {}

    logging.info(
        "Fetching HTML content from %s%s",
        url,
        "?" + "&".join([f"{k}={v}" for k, v in params.items()]) if params else "",
    )

    for attempt in range(0, retries):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()

            # Successful response, parse and return content
            bs = BeautifulSoup(response.content, "html.parser")
            # Introduce delay to be respectful to the server
            time.sleep(delay)
            logging.info(
                "Fetched HTML content from %s%s",
                url,
                (
                    "?" + "&".join([f"{k}={v}" for k, v in params.items()])
                    if params
                    else ""
                ),
            )
            return bs.prettify()
        except requests.Timeout as e:
            logging.error("Timeout accessing page: %s", e)
            logging.warning(
                "Attempt %d/%d failed due to timeout. Retrying...", attempt + 1, retries
            )
            logging.warning("Sleeping for %d seconds...", delay * (attempt + 1))
            time.sleep(delay * (attempt + 1))
        except requests.RequestException as e:
            logging.error("Error accessing page: %s", e)
            logging.warning("Attempt %d/%d failed. Retrying...", attempt + 1, retries)
        except Exception as e:
            logging.error("An unexpected error occurred: %s", e)
            raise RuntimeError(f"An unexpected error occurred: {e}") from e
        logging.warning("Sleeping for %d seconds...", delay * (attempt + 1))
        time.sleep(delay * (attempt + 1))

    # If all retries fail, raise an exception
    logging.error("Max retries reached. Failed to access page.")
    raise RuntimeError("Max retries reached. Failed to access page.")


def extract_categories():
    """
    Fetches and extracts categories and subcategories from Ahorramas main page.

    Returns:
    list: A list of dictionaries containing category information. Each dictionary has the following keys:
          - "name": The full category name (e.g., "Alimentación - Frutos Secos")
          - "url": The absolute URL of the category page
          - "parent_category": The main category name (e.g., "Alimentación")
          - "link_text": The text of the specific link

    This function uses the BeautifulSoup library to parse the HTML content of the Ahorramas main page.
    It extracts all category links from elements with class "menu-category-name" and returns them as a list.
    """
    logging.info("Starting extraction of categories from Ahorramas")

    url = "https://www.ahorramas.com/"

    html_content = fetch_html_content(url)
    soup = BeautifulSoup(html_content, "html.parser")

    urls = [
        link.get("href")
        for link in soup.find_all("a", string=lambda text: text and "Ver todo" in text)
    ]

    logging.info("Successfully extracted %d category URLs", len(urls))
    return urls


def extract_category_slugs() -> list[str]:
    """
    Extracts top-level category slugs from the category URLs returned by `extract_categories()`.

    The function calls `extract_categories()` to obtain the list of category URLs,
    parses each URL's path, and selects the first non-empty path segment as the
    top-level category. Both absolute and relative URLs are supported. Duplicates
    are removed and the result is returned sorted and lowercased.

    Returns:
    list[str]: Sorted list of unique top-level categories in lowercase
               (e.g., ["frescos", "alimentacion", "bebidas", "lacteos", "limpieza",
                       "hogar", "cuidado-personal", "congelados", "bebe", "mascotas"]).
    """
    urls = extract_categories()

    slugs: set[str] = set()
    for url in urls:
        path = urlparse(url).path
        segments = [s for s in path.split("/") if s]
        if segments:
            slugs.add(segments[0].lower())

    return sorted(slugs)


def get_category_total_size(category_url: str) -> int:
    """
    Extracts the total number of products from an Ahorramas category page.

    The function fetches the category URL HTML, locates the element that contains
    the text "<number> Resultados" (inside '.product-results-count'), and returns
    the integer value. It supports thousands separators like '.' or spaces.

    Parameters:
    category_url (str): The absolute URL of the Ahorramas category page
                        (e.g., "https://www.ahorramas.com/mascotas/").

    Returns:
    int: The total number of products found on the category page.
    """
    logging.info("Fetching total size from category page: %s", category_url)

    html_content = fetch_html_content(category_url)
    soup = BeautifulSoup(html_content, "html.parser")

    # Prefer the span inside the counter, fallback to the container itself
    counter_el = soup.select_one(".product-results-count span") or soup.select_one(
        ".product-results-count"
    )
    text = counter_el.get_text(strip=True) if counter_el else ""

    # Expected format: "<number> Resultados" -> take the first token
    first_token = text.split(" ", 1)[0]
    digits = re.sub(r"\D", "", first_token)  # normalize "2.311" -> "2311"
    return int(digits) if digits else 0


def extract_products(category_url: str, pmin: float = 0.01) -> pl.DataFrame:
    """
    Extracts all product information from a given Ahorramas category URL.

    The function first fetches the category page to get the total number of products,
    then calls the Search-UpdateGrid endpoint with start=0 and sz=total_size. It parses
    the returned HTML fragment and extracts, for each product:
        - discount-value
        - price
        - price-per-unit
        - name
        - image

    Parameters:
    category_url (str): The absolute URL of the Ahorramas category page
                        (e.g., "https://www.ahorramas.com/mascotas/").
    pmin (float, optional): Minimum price filter to avoid zero-priced items. Defaults to 0.01.

    Returns:
    pl.DataFrame: A Polars DataFrame with one row per product and the attributes above.
    """
    logging.info("Starting full extraction from category URL: %s", category_url)

    # Derive cgid from the last non-empty path segment of the URL (e.g., 'mascotas')
    path_parts = [p for p in urlparse(category_url).path.split("/") if p]
    cgid = path_parts[-1] if path_parts else ""

    total_size = get_category_total_size(category_url)
    logging.info("Total size detected for %s: %d", cgid, total_size)

    base_url = "https://www.ahorramas.com/on/demandware.store/Sites-Ahorramas-Site/es/Search-UpdateGrid"
    params = {
        "cgid": cgid,
        "pmin": f"{pmin:.2f}",
        "start": "0",
        "sz": str(total_size),
    }

    html_content = fetch_html_content(base_url, params=params, timeout=120, retries=5)
    soup = BeautifulSoup(html_content, "html.parser")

    def text_or_empty(el):
        return el.get_text(strip=True) if el else ""

    items = []
    for prod in soup.select("div.product"):
        tile_body = prod.select_one(".tile-body")

        discount_el = (
            tile_body.select_one(".discount-value .marker") if tile_body else None
        )
        discount_value = text_or_empty(discount_el)

        price_el = tile_body.select_one(".price .sales .value") if tile_body else None
        price = text_or_empty(price_el)

        unit_el = (
            tile_body.select_one(".unit-price-row .unit-price-per-unit")
            if tile_body
            else None
        )
        price_per_unit = text_or_empty(unit_el)

        name_el = prod.select_one(".pdp-link h2.link.product-name-gtm")
        name = text_or_empty(name_el)

        img_el = prod.select_one(".image-container img.tile-image")
        image = img_el.get("src") if img_el else ""

        items.append(
            {
                "discount-value": discount_value,
                "price": price,
                "price-per-unit": price_per_unit,
                "name": name,
                "image": image,
            }
        )

    df = pl.DataFrame(items)
    logging.info("Extracted %d products for cgid=%s", len(items), cgid)
    return df
