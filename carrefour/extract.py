import sys
import time
import logging
from os.path import dirname, abspath
import requests
import polars as pl
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

# Add the parent directory to the sys.path
sys.path.append(dirname(dirname(abspath(__file__))))


def read_xml_file() -> str:
    """
    Read supermarkets data from local XML file

    Returns:
        str: XML content as string
    """
    try:
        xml_path = dirname(abspath(__file__)) + "/locations.xml"
        logging.info("Reading supermarkets data from local XML file: %s", xml_path)

        with open(xml_path, "r", encoding="utf-8") as file:
            xml_content = file.read()

        logging.info("Successfully read XML file with %d characters", len(xml_content))
        return xml_content

    except FileNotFoundError:
        logging.error("XML file not found: %s", xml_path)
        raise FileNotFoundError(f"XML file not found: {xml_path}")
    except Exception as e:
        logging.error("Error reading XML file: %s", e)
        raise


def extract_supermarkets() -> pl.DataFrame:
    """
    Transform the Carrefour <marker/> data into a Polars DataFrame.

    Returns:
        pl.DataFrame: Columns -> store_id, address, schedule, holidays, latitude, longitude, name, category
    """
    # Read from local XML file instead of HTTP request
    raw_xml = read_xml_file()
    soup = BeautifulSoup(raw_xml, "html.parser")

    items = []

    # Directly parse <marker .../> nodes inside <markers>
    stores = soup.find_all("marker")

    # Helper to join address parts cleanly
    def join_addr(parts: list[str]) -> str:
        return ", ".join([p.strip() for p in parts if p and p.strip()])

    for m in stores:
        # Coordinates (required)
        lat = (m.get("lat") or "").strip()
        lng = (m.get("lng") or "").strip()
        if not lat or not lng:
            continue
        try:
            lat_f = float(lat)
            lng_f = float(lng)
            if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lng_f <= 180.0):
                continue
        except ValueError:
            continue

        # Stable store_id: prefer codsa, then codat, then tcm/id as fallback (no more from XML)
        store_id = (
            m.get("codsa") or m.get("codat") or m.get("tcm") or m.get("id") or ""
        ).strip()
        if not store_id:
            # As a last resort, hash lat/lng + address (no more from XML)
            store_id = f"{lat_f:.5f},{lng_f:.5f}"

        # Address: address + address2 + postal + city + state (province)
        address = join_addr(
            [
                m.get("address"),
                m.get("address2"),
                m.get("postal"),
                m.get("city"),
                m.get("state"),
            ]
        )

        # Schedule: hours1 | hours2 (if present)
        hours1 = (m.get("hours1") or "").strip()
        hours2 = (m.get("hours2") or "").strip()
        schedule = " | ".join([h for h in [hours1, hours2] if h])

        # Holidays not provided -> leave empty to match your staging schema
        holidays = ""

        # Additional fields from XML
        name = (m.get("name") or "").strip()
        category = (m.get("category") or "").strip()

        items.append(
            {
                "store_id": store_id,
                "address": address,
                "schedule": schedule,
                "holidays": holidays,
                "latitude": str(lat_f),
                "longitude": str(lng_f),
                "name": name,
                "category": category,
            }
        )

    # Deduplicate by store_id; if missing, fall back to lat/lng pair
    df = pl.DataFrame(items)
    if df.height > 0:
        if "store_id" in df.columns:
            df = df.unique(subset=["store_id"])
        else:
            df = df.unique(subset=["latitude", "longitude"])

    logging.info("Successfully extracted %d Carrefour stores", df.height)
    return df
