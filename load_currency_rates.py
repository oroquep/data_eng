import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def load_currency_rates():
    # This assumes we're running from root
    df = pd.read_csv('candidate_deliverables/currency_rates.csv')
    
    # Filter only latest rate per from_currency to GBP
    df = df[df['to_currency'] == 'GBP']
    df = df.sort_values('rate_date').drop_duplicates('from_currency', keep='last')

    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

    insert_query = """
        INSERT INTO currency_rates (from_currency, to_currency, rate, rate_date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (from_currency) DO UPDATE SET
            to_currency = EXCLUDED.to_currency,
            rate = EXCLUDED.rate,
            rate_date = EXCLUDED.rate_date
    """

    try:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute(insert_query, (
                    row['from_currency'],
                    row['to_currency'],
                    row['rate'],
                    row['rate_date']
                ))
        conn.commit()
        print("Currency rates loaded successfully.")

        # Select and print the first row to verify
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM currency_rates ORDER BY from_currency LIMIT 1")
            first_row = cur.fetchone()
            print("Sample row from currency_rates table:", first_row)
    except Exception as e:
        print(f"Error loading currency rates: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    load_currency_rates()