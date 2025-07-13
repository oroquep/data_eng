import sys
print(f"Running with Python: {sys.executable}")

import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def create_tables():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    
    commands = (
        """
        CREATE TABLE IF NOT EXISTS bookings (
            booking_id UUID PRIMARY KEY,
            check_in_date TIMESTAMP NOT NULL,
            check_out_date TIMESTAMP NOT NULL,
            owner_company TEXT NOT NULL,
            owner_company_country TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS currency_rates (
            from_currency TEXT PRIMARY KEY,
            to_currency TEXT NOT NULL,
            rate NUMERIC NOT NULL,
            rate_date DATE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS monthly_revenue (
            month DATE NOT NULL,
            owner_company TEXT NOT NULL,
            original_currency TEXT NOT NULL,
            revenue NUMERIC NOT NULL,
            revenue_gbp NUMERIC NOT NULL,
            PRIMARY KEY (month, owner_company)
        )
        """
    )
    
    try:
        with conn.cursor() as cur:
            for command in commands:
                cur.execute(command)
        conn.commit()
    except Exception as e:
        print(f"Error creating tables: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    create_tables()
