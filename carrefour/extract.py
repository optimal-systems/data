import sys
import logging
from os.path import dirname, abspath
import polars as pl
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import re
import json
import os
from pathlib import Path

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


def extract_categories_from_html(html: str) -> list[dict]:
    """
    Extrae categorías top del HTML de /supermercado.
    Devuelve: [{name, slug, cat_id, url}]
    """
    base_url = "https://www.carrefour.es"
    soup = BeautifulSoup(html, "html.parser")
    nav = soup.find("nav", class_="home-food-view__category-SEO-links")
    links = nav.select("a[href]") if nav else []
    out = []

    for a in links:
        name = a.get_text(strip=True)
        href = a.get("href") or ""
        if not name or not href:
            continue
        url = urljoin(base_url, href)
        path = urlparse(href).path.strip("/")
        parts = [p for p in path.split("/") if p]
        slug = parts[1] if len(parts) > 1 else (parts[0] if parts else "")
        m = re.search(r"(cat\d+)", href)
        cat_id = m.group(1) if m else ""
        out.append({"name": name, "slug": slug, "cat_id": cat_id, "url": url})
    return out


def extract_category_slugs_from_html(html: str) -> list[str]:
    """Devuelve solo los slugs únicos y ordenados."""
    cats = extract_categories_from_html(html)
    return sorted({c["slug"].lower() for c in cats if c.get("slug")})


def extract_products_from_html_files(raw_data_dir: str = None) -> pl.DataFrame:
    """
    Extract products from all HTML files in the raw_data directory.

    Args:
        raw_data_dir: Directory containing HTML files. If None, uses default directory.

    Returns:
        pl.DataFrame: DataFrame with extracted products
    """
    if raw_data_dir is None:
        raw_data_dir = dirname(abspath(__file__)) + "/raw_data"

    if not os.path.exists(raw_data_dir):
        logging.error("Raw data directory not found: %s", raw_data_dir)
        raise FileNotFoundError(f"Raw data directory not found: {raw_data_dir}")

    all_products = []
    html_files = list(Path(raw_data_dir).glob("*.html"))

    logging.info("Found %d HTML files to process", len(html_files))

    for html_file in html_files:
        try:
            logging.info("Processing file: %s", html_file.name)
            products = extract_products_from_single_html(html_file)
            all_products.extend(products)
            logging.info(
                "Extracted %d products from file %s", len(products), html_file.name
            )
        except Exception as e:
            logging.error("Error processing file %s: %s", html_file.name, e)
            continue

    if not all_products:
        logging.warning("No products found in any HTML file")
        return pl.DataFrame()

    # Create DataFrame and remove duplicates
    df = pl.DataFrame(all_products)

    # Remove duplicates based on name and supermarket
    df = df.unique(subset=["name", "supermarket"], keep="first")

    logging.info("Total unique products extracted: %d", df.height)
    return df


def extract_products_from_single_html(html_file_path: Path) -> list[dict]:
    """
    Extract products from a single HTML file.

    Args:
        html_file_path: Path to the HTML file

    Returns:
        list[dict]: List of extracted products
    """
    try:
        with open(html_file_path, "r", encoding="utf-8") as file:
            html_content = file.read()

        # Search for the JavaScript 'impressions' array that contains the products
        impressions_match = re.search(
            r'window\["impressions"\]\s*=\s*(\[.*?\]);', html_content, re.DOTALL
        )

        if not impressions_match:
            logging.warning("Array 'impressions' not found in %s", html_file_path.name)
            return []

        # Extract and parse JSON
        impressions_json = impressions_match.group(1)
        products_data = json.loads(impressions_json)

        # Extract category information from filename
        filename = html_file_path.name
        category = extract_category_from_filename(filename)

        # Parse HTML and build a map from slug (in href) to {name, url}
        soup = BeautifulSoup(html_content, "html.parser")
        product_links = soup.find_all("h2", class_="product-card__title")

        # slug -> {"name": human_text, "url": href}
        product_map: dict[str, dict[str, str]] = {}

        for h2 in product_links:
            a_tag = h2.find("a")
            if not a_tag:
                continue

            human_text = (a_tag.get_text(strip=True) or "").strip()
            href = (a_tag.get("href") or "").strip()
            if not human_text or not href:
                continue

            # Derive slug from href path:
            # Example href:
            # /supermercado/toallitas-humedas-higienicas-infantiles-carrefour-soft-100-ud/R-VC4AECOMM-002344/p
            path = urlparse(href).path
            parts = [
                p for p in path.split("/") if p
            ]  # ["supermercado", "<slug>", "R-....", "p"]
            slug = parts[1] if len(parts) >= 2 else ""

            if slug and slug not in product_map:
                product_map[slug] = {"name": human_text, "url": href}

        products: list[dict] = []

        for product in products_data:
            # JSON gives the slug in 'item_name' (e.g., 'toallitas-humedas-higienicas-infantiles-carrefour-soft-100-ud')
            json_slug = str(product.get("item_name", "")).strip()

            # Get human-friendly name and URL from HTML via slug derived from href
            mapped = product_map.get(json_slug, {})
            human_name = mapped.get("name", "")
            product_url = mapped.get("url", "")

            # Fallback: if we didn't find it in HTML, create a readable name from the slug
            if not human_name and json_slug:
                human_name = json_slug.replace("-", " ").strip().capitalize()

            clean_product = {
                "discount_value": str(product.get("coupon", "")),
                "price": float(product.get("price", 0.0)),
                "price_per_unit": str(product.get("item_variant", "")),
                "name": human_name,  # texto del <a>
                "image": "",  # no disponible aquí
                "url": product_url,  # href del <a> (relativa)
                "supermarket": "carrefour",
                "source_file": filename,
                "extracted_category": category,
            }
            products.append(clean_product)

        return products

    except json.JSONDecodeError as e:
        logging.error("Error parsing JSON products in %s: %s", html_file_path.name, e)
        return []
    except Exception as e:
        logging.error("Unexpected error processing %s: %s", html_file_path.name, e)
        return []


def extract_category_from_filename(filename: str) -> str:
    """
    Extract category from HTML filename.

    Args:
        filename: HTML filename

    Returns:
        str: Category extracted from filename
    """
    # Mapping of filename patterns to readable categories
    category_mapping = {
        "productos-frescos": "Productos Frescos",
        "la-despensa": "La Despensa",
        "parafarmacia": "Parafarmacia",
        "mascotas": "Mascotas",
        "bebe": "Bebé",
    }

    for key, value in category_mapping.items():
        if key in filename.lower():
            return value

    # If no match, extract from filename
    if "cat20002" in filename:
        return "Productos Frescos"
    elif "cat20001" in filename:
        return "La Despensa"
    elif "cat20008" in filename:
        return "Parafarmacia"
    elif "cat20007" in filename:
        return "Mascotas"
    elif "cat20006" in filename:
        return "Bebé"

    return "Unknown"


def get_product_statistics(df: pl.DataFrame) -> dict:
    """
    Get statistics from extracted products.

    Args:
        df: DataFrame with products

    Returns:
        dict: Dictionary with statistics
    """
    if df.height == 0:
        return {"total_products": 0}

    stats = {
        "total_products": df.height,
        "total_categories": df["extracted_category"].n_unique(),
        "categories": df.group_by("extracted_category")
        .agg(pl.count().alias("count"))
        .to_dicts(),
        "price_range": {
            "min": float(df["price"].min()),
            "max": float(df["price"].max()),
            "avg": float(df["price"].mean()),
        },
        "discounts_count": df.filter(pl.col("discount_value") != "").height,
        "files_processed": df["source_file"].n_unique(),
    }

    return stats
