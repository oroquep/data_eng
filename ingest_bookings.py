import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

API_URL = "http://localhost:5000/api/bookings"

# Map countries to currency codes
COUNTRY_CURRENCY_MAP = {
    "UK": "GBP",
    "USA": "USD",
    "France": "EUR",
    
}

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

def fetch_bookings(page=1, per_page=100):
    params = {"page": page, "per_page": per_page}
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    return response.json()

def main():
    conn = get_db_connection()
    cursor = conn.cursor()

    page = 1
    per_page = 100
    total = None
    inserted_count = 0

    print("Starting bookings ingestion...")

    try:
        while True:
            data = fetch_bookings(page=page, per_page=per_page)
            bookings = data["results"]
            total = data["total"]

            if not bookings:
                break

            # Prepare data for batch insert
            records = []
            for b in bookings:
                currency = COUNTRY_CURRENCY_MAP.get(b["owner_company_country"])
                if not currency:
                    print(f"Warning: Currency not found for country {b['owner_company_country']}, skipping booking {b['booking_id']}")
                    continue
                records.append((
                    b["booking_id"],
                    b["check_in_date"],
                    b["check_out_date"],
                    b["owner_company"],
                    b["owner_company_country"],
                    currency
                ))

            # Insert bookings with ON CONFLICT DO NOTHING to avoid duplicates
            insert_query = """
            INSERT INTO bookings (booking_id, check_in_date, check_out_date, owner_company, owner_company_country, currency)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (booking_id) DO NOTHING
            """
            execute_batch(cursor, insert_query, records)
            conn.commit()

            inserted_count += len(records)
            print(f"Page {page} processed, inserted {len(records)} bookings.")

            if page * per_page >= total:
                break
            page += 1

        print(f"Ingestion complete. Total bookings inserted: {inserted_count}")

        # Sanity check: print first 5 bookings from DB
        print("\nSample bookings from DB:")
        cursor.execute("SELECT booking_id, check_in_date, check_out_date, owner_company, owner_company_country, currency FROM bookings LIMIT 5;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    except Exception as e:
        print(f"Error during ingestion: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()