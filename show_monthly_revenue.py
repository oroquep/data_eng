import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def show_monthly_revenue():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT month, owner_company, original_currency, revenue, revenue_gbp
                FROM monthly_revenue
                ORDER BY month, owner_company
            """)
            rows = cur.fetchall()
            print(f"{'Month':<12} | {'Owner Company':<30} | {'Currency':<8} | {'Revenue':>10} | {'Revenue GBP':>12}")
            print("-" * 80)
            for month, owner, curr, rev, rev_gbp in rows:
                print(f"{month:%Y-%m}   | {owner:<30} | {curr:<8} | {rev:10.2f} | {rev_gbp:12.2f}")
    finally:
        conn.close()

if __name__ == '__main__':
    show_monthly_revenue()