import os
import time
import logging
from typing import List, Tuple

import requests
import psycopg2

from dotenv import load_dotenv
from psycopg2.extras import execute_values
from psycopg2 import OperationalError, InterfaceError, DatabaseError


load_dotenv()


# =========================
# Configuration
# =========================
CITIES = {
    "Jakarta": {"lat": -6.2, "lon": 106.8},
    "Bandung": {"lat": -6.9, "lon": 107.6},
    "Surabaya": {"lat": -7.25, "lon": 112.75},
    "Medan": {"lat": 3.59, "lon": 98.67},
    "Yogyakarta": {"lat": -7.8, "lon": 110.37},
    "Denpasar": {"lat": -8.65, "lon": 115.22},
}

FORECAST_DAYS = int(os.getenv("FORECAST_DAYS", "3"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Jakarta")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

API_TIMEOUT = int(os.getenv("API_TIMEOUT", "20"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "3"))
API_BASE_DELAY = int(os.getenv("API_BASE_DELAY", "3"))

DB_MAX_RETRIES = int(os.getenv("DB_MAX_RETRIES", "3"))
DB_BASE_DELAY = int(os.getenv("DB_BASE_DELAY", "3"))

DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"


# =========================
# Logging Setup
# =========================
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# =========================
# Helpers
# =========================
def exponential_backoff(base_delay: int, attempt: int) -> int:
    """
    Return exponential backoff delay in seconds.
    Attempt starts at 1.
    """
    return base_delay * (2 ** (attempt - 1))


def validate_env() -> None:
    """
    Ensure all required database environment variables are present.
    """
    required_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT"]
    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


def get_db_connection():
    """
    Create and return a PostgreSQL connection.
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT"),
        sslmode="require"
    )


def extract_weather(city_name: str, lat: float, lon: float) -> dict:
    """
    Fetch hourly weather data from Open-Meteo with retry logic.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m",
        "timezone": TIMEZONE,
        "forecast_days": FORECAST_DAYS,
    }

    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            logger.info(
                "Fetching weather data for %s (attempt %s/%s)",
                city_name, attempt, API_MAX_RETRIES
            )

            response = requests.get(url, params=params, timeout=API_TIMEOUT)
            response.raise_for_status()

            logger.info("Successfully fetched data for %s", city_name)
            return response.json()

        except requests.RequestException as e:
            logger.warning("API request failed for %s: %s", city_name, e)

            if attempt < API_MAX_RETRIES:
                delay = exponential_backoff(API_BASE_DELAY, attempt)
                logger.info(
                    "Retrying API request for %s in %s seconds...",
                    city_name, delay
                )
                time.sleep(delay)
            else:
                logger.error(
                    "API request failed after %s attempts for %s",
                    API_MAX_RETRIES, city_name
                )
                raise


def prepare_rows(city_name: str, data: dict) -> List[Tuple[str, str, float, float]]:
    """
    Convert API response into a list of rows for database insertion.
    """
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temperatures = hourly.get("temperature_2m", [])
    humidities = hourly.get("relative_humidity_2m", [])

    if not (len(times) == len(temperatures) == len(humidities)):
        raise ValueError(f"Mismatched hourly data lengths for {city_name}")

    rows = [
        (city_name, times[i], temperatures[i], humidities[i])
        for i in range(len(times))
    ]

    return rows


def insert_rows_with_retry(conn, cur, city_name: str, rows: List[Tuple[str, str, float, float]]):
    """
    Insert rows into PostgreSQL using batch insert with retry logic.
    Returns updated connection, cursor, and processed row count.
    """
    if not rows:
        logger.info("No rows to insert for %s", city_name)
        return conn, cur, 0

    query = """
        INSERT INTO weather_raw (city, time, temperature, humidity)
        VALUES %s
        ON CONFLICT (city, time) DO NOTHING
    """

    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            logger.info(
                "Inserting %s rows for %s (attempt %s/%s)",
                len(rows), city_name, attempt, DB_MAX_RETRIES
            )

            execute_values(cur, query, rows, page_size=BATCH_SIZE)
            conn.commit()

            logger.info("Successfully inserted rows for %s", city_name)
            return conn, cur, len(rows)

        except (OperationalError, InterfaceError, DatabaseError) as e:
            logger.warning("Database insert failed for %s: %s", city_name, e)

            try:
                conn.rollback()
            except Exception:
                pass

            if attempt < DB_MAX_RETRIES:
                delay = exponential_backoff(DB_BASE_DELAY, attempt)
                logger.info(
                    "Retrying database insert for %s in %s seconds...",
                    city_name, delay
                )

                try:
                    cur.close()
                except Exception:
                    pass

                try:
                    conn.close()
                except Exception:
                    pass

                time.sleep(delay)

                conn = get_db_connection()
                cur = conn.cursor()
            else:
                logger.error(
                    "Database insert failed after %s attempts for %s",
                    DB_MAX_RETRIES, city_name
                )
                raise

    return conn, cur, 0


def run_etl() -> None:
    """
    Main ETL process.
    """
    validate_env()
    logger.info("Multi-city weather ETL started")

    conn = None
    cur = None
    total_processed = 0

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for city_name, coords in CITIES.items():
            logger.info("Starting ETL for %s", city_name)

            data = extract_weather(city_name, coords["lat"], coords["lon"])
            rows = prepare_rows(city_name, data)

            logger.info("Prepared %s rows for %s", len(rows), city_name)

            conn, cur, processed = insert_rows_with_retry(conn, cur, city_name, rows)
            total_processed += processed

            logger.info("%s completed | processed rows: %s", city_name, processed)

        logger.info("ETL finished successfully")
        logger.info("Total processed rows: %s", total_processed)

    except Exception as e:
        logger.exception("ETL failed: %s", e)
        raise

    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass

        if conn:
            try:
                conn.close()
            except Exception:
                pass

        logger.info("Database connection closed")


if __name__ == "__main__":
    run_etl()
