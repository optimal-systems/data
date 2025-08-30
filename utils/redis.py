import hashlib
import os
from dotenv import load_dotenv
import redis

# Load environment variables from .env file
load_dotenv()

_REDIS_CONN = None


def redis_conn() -> redis.Redis:
    """
    Establishes and returns a Redis connection.

    This function checks if a global Redis connection object (_redis_conn) is already
    established. If not, it initializes the connection using the host and port
    specified in the environment variables 'REDIS_HOST' and 'REDIS_PORT'. The connection
    is then returned for use.

    Returns:
        redis.Redis: A Redis connection object.
    """
    global _REDIS_CONN
    if _REDIS_CONN is None:
        _REDIS_CONN = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=0,
        )
    return _REDIS_CONN


def hash_md5(data: str) -> str:
    """
    Calculates the MD5 hash of the provided data.

    Parameters:
    data (str): The input data for which the MD5 hash needs to be calculated.

    Returns:
    str: The MD5 hash of the input data in hexadecimal format.
    """
    md5_hash = hashlib.md5()
    md5_hash.update(data.encode("utf-8"))
    return md5_hash.hexdigest()
