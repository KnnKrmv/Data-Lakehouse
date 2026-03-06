from faker import Faker
import psycopg2
import random
from io import StringIO
from datetime import datetime, timedelta
import time

# =========================
# CONFIG
# =========================
DB_CONFIG = {
    "host": "localhost",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
    "port": 5432,
    "options": "-c search_path=sales"
}

BATCH_SIZE = 500_000
TOTAL_BATCHES = 2000   # 500,000 * 2000 = 1,000,000,000 rows

# Kiçik test üçün:
# BATCH_SIZE = 100_000
# TOTAL_BATCHES = 5

fake = Faker()

# =========================
# STATIC DIMENSIONS
# =========================
products = [
    "Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse",
    "Headphones", "Printer", "Camera", "Speaker", "Smart Watch",
    "Router", "Microphone", "Projector", "SSD"
]

categories = [
    "Electronics", "Accessories", "Office", "Gaming", "Networking"
]

payments = [
    "Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash"
]

channels = [
    "Online", "Retail Store", "Mobile App", "Marketplace"
]

shipment = [
    "Standard", "Express", "Same Day"
]

segments = [
    "Consumer", "Corporate", "Small Business"
]

currencies = [
    "USD", "EUR", "GBP", "AZN"
]

# =========================
# HELPERS
# =========================
def clean_text(value: str) -> str:
    return str(value).replace("\t", " ").replace("\n", " ").replace("\r", " ")

def build_pool(generator_func, size: int):
    return [clean_text(generator_func()) for _ in range(size)]

def format_num(x: int) -> str:
    return f"{x:,}"

# =========================
# PREGENERATED FAKER POOLS
# =========================
print("Preparing faker pools...")

NAMES = build_pool(fake.name, 10_000)
EMAILS = build_pool(fake.email, 10_000)
COUNTRIES = build_pool(fake.country, 300)
CITIES = build_pool(fake.city, 3_000)
SALESPERSONS = build_pool(fake.name, 2_000)

print("Pools ready.")

# =========================
# DATA GENERATION
# =========================
base_date = datetime(2020, 1, 1)

def generate_batch(batch_size: int) -> StringIO:
    buffer = StringIO()
    write = buffer.write

    for _ in range(batch_size):
        price = random.randint(20, 3000)
        quantity = random.randint(1, 10)

        transaction_amount = round(price * quantity, 2)
        discount_amount = round(transaction_amount * random.uniform(0, 0.20), 2)
        tax_amount = round((transaction_amount - discount_amount) * 0.18, 2)
        total_amount = round(transaction_amount - discount_amount + tax_amount, 2)

        transaction_date = base_date + timedelta(
            days=random.randint(0, 2200),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )

        row = [
            f"{transaction_amount:.2f}",
            str(quantity),
            f"{discount_amount:.2f}",
            f"{tax_amount:.2f}",
            f"{total_amount:.2f}",
            random.choice(NAMES),
            random.choice(EMAILS),
            random.choice(COUNTRIES),
            random.choice(CITIES),
            random.choice(products),
            random.choice(categories),
            random.choice(payments),
            random.choice(channels),
            random.choice(shipment),
            random.choice(SALESPERSONS),
            random.choice(segments),
            random.choice(currencies),
            transaction_date.strftime("%Y-%m-%d %H:%M:%S"),
        ]

        write("\t".join(row) + "\n")

    buffer.seek(0)
    return buffer

# =========================
# MAIN LOAD
# =========================
def main():
    start_time = time.time()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        # Speed settings
        cur.execute("SET synchronous_commit TO OFF;")
        cur.execute("SET work_mem TO '128MB';")

        total_inserted = 0

        for i in range(TOTAL_BATCHES):
            batch_start = time.time()

            data = generate_batch(BATCH_SIZE)

            cur.copy_from(
                data,
                "salesorders",
                columns=(
                    "transaction_amount",
                    "quantity",
                    "discount_amount",
                    "tax_amount",
                    "total_amount",
                    "customer_name",
                    "customer_email",
                    "country",
                    "city",
                    "product_name",
                    "product_category",
                    "payment_method",
                    "sales_channel",
                    "shipment_type",
                    "salesperson",
                    "customer_segment",
                    "currency",
                    "transaction_date"
                )
            )

            conn.commit()

            total_inserted += BATCH_SIZE
            batch_elapsed = time.time() - batch_start
            total_elapsed = time.time() - start_time

            print(
                f"Batch {i + 1}/{TOTAL_BATCHES} inserted | "
                f"Batch rows: {format_num(BATCH_SIZE)} | "
                f"Total rows: {format_num(total_inserted)} | "
                f"Batch time: {batch_elapsed:.2f}s | "
                f"Total time: {total_elapsed/60:.2f} min"
            )

    except Exception as e:
        conn.rollback()
        print("Error occurred:", e)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()