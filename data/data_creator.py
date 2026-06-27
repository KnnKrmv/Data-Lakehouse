from faker import Faker
import psycopg2
import random
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

# =========================
# CONFIG
# =========================
DB_CONFIG = {
    "host": "localhost",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
    "port": 5432
}

fake = Faker()

# =========================
# POOLS
# =========================
PRODUCTS = [
    "Laptop", "Phone", "Tablet",
    "Monitor", "Keyboard", "Mouse",
    "SSD", "Headphones"
]

CATEGORIES = [
    "Electronics",
    "Accessories",
    "Gaming",
    "Office"
]

CURRENCIES = ["USD", "EUR", "AZN"]

PAYMENT_METHODS = [
    "Credit Card",
    "Cash",
    "Bank Transfer",
    "Apple Pay"
]

STATUSES = [
    "Completed",
    "Pending",
    "Cancelled"
]

# =========================
# DATA GENERATOR
# =========================
def generate_row():
    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(10, 3000), 2)

    return (
        random.randint(1000, 50000),                # customer_id
        fake.name(),                                # customer_name
        random.randint(1, 5000),                    # product_id
        random.choice(PRODUCTS),                    # product_name
        random.choice(CATEGORIES),                  # product_category
        quantity,
        unit_price,
        round(quantity * unit_price, 2),            # total_amount
        random.choice(CURRENCIES),
        random.choice(PAYMENT_METHODS),
        random.choice(STATUSES),
        datetime(2020, 1, 1) +
        timedelta(days=random.randint(0, 2200))
    )

# =========================
# MAIN
# =========================
def main():

    TOTAL_ROWS = 100_000
    BATCH_SIZE = 20000

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    insert_sql = """
    INSERT INTO sales.sales_transactions (
        customer_id,
        customer_name,
        product_id,
        product_name,
        product_category,
        quantity,
        unit_price,
        total_amount,
        currency,
        payment_method,
        transaction_status,
        transaction_date
    )
    VALUES %s
    """

    for batch_start in range(0, TOTAL_ROWS, BATCH_SIZE):

        rows = [
            generate_row()
            for _ in range(BATCH_SIZE)
        ]

        execute_values(
            cur,
            insert_sql,
            rows
        )

        conn.commit()

        print(
            f"Inserted "
            f"{min(batch_start + BATCH_SIZE, TOTAL_ROWS)} "
            f"/ {TOTAL_ROWS}"
        )

    cur.close()
    conn.close()

    print("✅ 100000 rows inserted")

if __name__ == "__main__":
    main()