import csv
import random
import string
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal

# --- Configuration ---
OUTPUT_FILE = "synthetic_data.csv"
NUM_ROWS = 1_000_000

# Helper generators
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_int(low=0, high=10_000):
    return random.randint(low, high)

def random_date(start_year=2000, end_year=2025):
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def random_datetime():
    start = datetime(2000, 1, 1)
    end = datetime(2025, 12, 31, 23, 59, 59)
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def random_datetime_tz():
    tz_offset = random.randint(-12, 14)  # UTC offset range
    tz = timezone(timedelta(hours=tz_offset))
    return random_datetime().replace(tzinfo=tz)

def random_decimal():
    return Decimal(f"{random.randint(0, 10_000)}.{random.randint(0, 99):02d}")

# Column names
columns = [
    "id", "name", "age", "signup_date", "last_login",
    "purchase_ts_tz", "balance", "rating", "country",
    "city", "zipcode", "flag", "created_dt", "updated_dt_tz", "score"
]

# --- Write CSV ---
with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(columns)

    for i in range(NUM_ROWS):
        row = [
            i + 1,                                 # id (int)
            random_string(12),                     # name (string)
            random_int(18, 80),                    # age (int)
            random_date(),                         # signup_date (date)
            random_datetime(),                     # last_login (datetime)
            random_datetime_tz(),                  # purchase_ts_tz (datetime w/ tz)
            random_decimal(),                      # balance (decimal)
            round(random.uniform(0, 5), 2),        # rating (float)
            random.choice(["US", "IN", "UK", "DE", "FR"]),  # country
            random_string(8),                      # city (string)
            random_int(10000, 99999),              # zipcode (int)
            random.choice([True, False]),          # flag (boolean)
            random_datetime(),                     # created_dt (datetime)
            random_datetime_tz(),                  # updated_dt_tz (datetime w/ tz)
            random_decimal()                       # score (decimal)
        ]
        writer.writerow(row)

print(f"CSV file '{OUTPUT_FILE}' with {NUM_ROWS:,} rows generated successfully.")


id,name,age,signup_date,last_login,purchase_ts_tz,balance,rating,country,city,zipcode,flag,created_dt,updated_dt_tz,score