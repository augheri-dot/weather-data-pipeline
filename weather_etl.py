import requests
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

CITIES = {
    "Jakarta": {"lat": -6.2, "lon": 106.8},
    "Bandung": {"lat": -6.9, "lon": 107.6},
    "Surabaya": {"lat": -7.25, "lon": 112.75},
    "Medan": {"lat": 3.59, "lon": 98.67},
    "Yogyakarta": {"lat": -7.8, "lon": 110.37},
    "Denpasar": {"lat": -8.65, "lon": 115.22},
}

def extract_weather(city_name, lat, lon):
    print(f"Mulai ambil data cuaca untuk {city_name}...")

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m",
        "timezone": "Asia/Jakarta"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    print(f"Berhasil ambil data untuk {city_name}")
    return data

def load_to_db(city_name, data, cur):
    hourly = data["hourly"]

    inserted = 0
    skipped = 0
    total_rows = len(hourly["time"])

    print(f"Mulai load data {city_name} ke database...")

    for i in range(total_rows):
        time = hourly["time"][i]
        temp = hourly["temperature_2m"][i]
        humidity = hourly["relative_humidity_2m"][i]

        cur.execute(
            """
            INSERT INTO weather_raw (city, time, temperature, humidity)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (city, time) DO NOTHING;
            """,
            (city_name, time, temp, humidity)
        )

        if cur.rowcount == 1:
            inserted += 1
        else:
            skipped += 1

        if (i + 1) % 50 == 0 or (i + 1) == total_rows:
            print(f"{city_name}: {i + 1}/{total_rows} baris diproses")

    return inserted, skipped

def main():
    print("ETL multi-kota dimulai...")

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT"),
            sslmode="require"
        )

        cur = conn.cursor()

        total_inserted = 0
        total_skipped = 0

        for city_name, coords in CITIES.items():
            data = extract_weather(city_name, coords["lat"], coords["lon"])
            inserted, skipped = load_to_db(city_name, data, cur)

            total_inserted += inserted
            total_skipped += skipped

            print(f"{city_name} -> Inserted: {inserted}, Skipped: {skipped}")

        conn.commit()

        print("ETL selesai")
        print(f"Total Inserted: {total_inserted}")
        print(f"Total Skipped: {total_skipped}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Terjadi error: {e}")
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Koneksi database ditutup")

if __name__ == "__main__":
    main()