import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime
from collections import defaultdict

load_dotenv()

MIN_FEES_BY_CURRENCY = {
    "GBP": 100,
    "USD": 140,
    "EUR": 120
}

def get_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def fetch_bookings(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT booking_id, check_out_date, owner_company, currency
            FROM bookings
        """)
        return cur.fetchall()

def fetch_fx_rates(conn):
    with conn.cursor() as cur:
        # Fetch all rates ordered by from_currency, to_currency, rate_date DESC (latest first)
        cur.execute("""
            SELECT from_currency, to_currency, rate, rate_date
            FROM currency_rates
            ORDER BY from_currency, to_currency, rate_date DESC
        """)
        rows = cur.fetchall()

    # Structure FX rates in a dict keyed by (from_currency, to_currency, rate_date)
    rates = defaultdict(list)
    for from_curr, to_curr, rate, rate_date in rows:
        rates[(from_curr, to_curr)].append((rate_date, rate))

    return rates

def find_rate_on_or_before(rates_list, target_date):
    """
    Find the rate with the closest date <= target_date
    rates_list is list of (rate_date, rate) sorted DESC by date
    """
    for rate_date, rate in rates_list:
        if rate_date <= target_date:
            return rate
    return None  # No rate found before target_date

def compute_monthly_revenue():
    conn = get_connection()
    try:
        bookings = fetch_bookings(conn)
        fx_rates = fetch_fx_rates(conn)

        print(f"Total bookings fetched: {len(bookings)}")
        print(f"Total currency rates fetched: {dict(fx_rates)}")

        # Aggregate fees by (month, owner_company, currency)
        fees_by_month_company_currency = defaultdict(float)

        for booking_id, check_out_date, owner_company, currency in bookings:
            print(f"{booking_id} - {owner_company} - {check_out_date}")
            
            # Determine booking month (year-month) from check_out_date
            month = check_out_date.replace(day=1).date()

            # Fee is fixed 10 GBP / 14 USD / 12 EUR per booking (based on currency)
            # So the original booking fee in original currency is:
            if currency == "GBP":
                fee = 10
            elif currency == "USD":
                fee = 14
            elif currency == "EUR":
                fee = 12
            else:
                # If currency not known, skip booking or treat fee as 0
                fee = 0

            fees_by_month_company_currency[(month, owner_company, currency)] += fee

        # Also gather all distinct (month, owner_company, currency) to apply minimum fee for months with no bookings
        # But for now, we’ll just apply minimum fee if sum is below threshold.

        # Calculate revenue in GBP applying FX rates and minimum fees
        monthly_revenue = []

        for (month, owner_company, currency), total_fee in fees_by_month_company_currency.items():
            # Apply minimum fee if below threshold
            min_fee = MIN_FEES_BY_CURRENCY.get(currency, 0)
            total_fee = max(total_fee, min_fee)

            # Find FX rate from currency to GBP for this checkout month date
            # Use checkout date = month (first day of month)
            rates_list = fx_rates.get((currency, "GBP"), [])

            rate = find_rate_on_or_before(rates_list, month)

            if rate is None:
                print(f"No FX rate found for {currency} to GBP on or before {month}, skipping...")
                continue
            else:
                print(f"FX rate found for {currency} to GBP on {month} with rate {rate}.")

            revenue_gbp = total_fee * float(rate)

            monthly_revenue.append((
                month,
                owner_company,
                currency,
                total_fee,
                revenue_gbp
            ))

        # Insert or update monthly_revenue table
        with conn.cursor() as cur:
            for month, owner_company, currency, revenue, revenue_gbp in monthly_revenue:
                cur.execute("""
                    INSERT INTO monthly_revenue (month, owner_company, original_currency, revenue, revenue_gbp)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (month, owner_company)
                    DO UPDATE SET original_currency = EXCLUDED.original_currency,
                                  revenue = EXCLUDED.revenue,
                                  revenue_gbp = EXCLUDED.revenue_gbp
                """, (month, owner_company, currency, revenue, revenue_gbp))
            conn.commit()

        print("Monthly revenue computed and stored successfully.")

    except Exception as e:
        print(f"Error computing monthly revenue: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    compute_monthly_revenue()