from urllib.parse import urlencode
import requests
import time
import logging
from bs4 import BeautifulSoup
import sys
from os.path import dirname, abspath

# Add the parent directory to the sys.path
sys.path.append(dirname(dirname(abspath(__file__))))

from utils.redis import redis_conn, hash_md5


def _build_full_url(url: str, params: dict | None) -> str:
    """
    Builds a stable, fully-qualified URL string including a sorted querystring
    to guarantee deterministic cache keys.
    """
    if not params:
        return url
    # sort params to avoid duplicated cache entries due to param order
    query = urlencode(sorted((k, str(v)) for k, v in params.items()))
    return f"{url}?{query}"


def fetch_html_content(url, params=None, retries=5, timeout=5, delay=5):
    """
    Fetches HTML content from the specified URL using the requests library.
    If the request fails due to a timeout or other request exception,
    it will retry the specified number of times.

    The response is cached in Redis under the 'urls' collection using a key
    derived from the MD5 hash of the fully-qualified URL (including querystring).
    Subsequent calls with the same URL+params return the cached HTML.

    Parameters:
    url (str): The URL to fetch the HTML content from.
    params (dict, optional): Additional parameters to be sent with the request. Defaults to None.
    retries (int, optional): The number of times to retry the request in case of failure.
                             Defaults to 5.
    timeout (int, optional): The timeout for the request in seconds. Defaults to 5.
    delay (int, optional): The delay between retries in seconds. Defaults to 5.

    Returns:
    str: The prettified HTML content of the fetched page (from cache if available).

    Raises:
    RuntimeError: If the maximum number of retries is reached without successful response.
    """
    if params is None:
        params = {}

    full_url = _build_full_url(url, params)
    redis_key = f"urls:{hash_md5(full_url)}"

    # Cache hit: return stored HTML immediately
    if redis_conn().exists(redis_key):
        cached = redis_conn().hgetall(redis_key)
        html_bytes = cached.get(b"html")
        if html_bytes:
            logging.info("Cache hit for %s", full_url)
            return html_bytes.decode("utf-8")

    logging.info("Fetching HTML content from %s", full_url)

    for attempt in range(0, retries):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()

            bs = BeautifulSoup(response.content, "html.parser")
            html_pretty = bs.prettify()

            # Be respectful to the server
            time.sleep(delay)
            logging.info("Fetched HTML content from %s", full_url)

            # Store in Redis (collection 'urls')
            redis_conn().hset(
                redis_key,
                mapping={
                    "url": full_url,
                    "html": html_pretty,
                },
            )

            return html_pretty

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

    logging.error("Max retries reached. Failed to access page.")
    raise RuntimeError("Max retries reached. Failed to access page.")
