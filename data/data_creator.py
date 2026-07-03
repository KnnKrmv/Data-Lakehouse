from faker import Faker
import psycopg2
import random
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

# =========================
# DATABASE CONFIG
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
# SETTINGS
# =========================
TOTAL_ROWS = 100_000
BATCH_SIZE = 20_000

STATUSES = [
    "Completed",
    "Pending",
    "Cancelled"
]

# =========================
# GENERATE ONE ROW
# =========================
def generate_row():
    quantity = random.randint(1, 20)
    price = round(random.uniform(10, 1000), 2)

    return (
        random.randint(1, 50000),                           # customer_id
        random.randint(1, 5000),                            # product_id
        quantity,                                           # quantity
        round(quantity * price, 2),                         # amount
        fake.date_time_between(
            start_date="-3y",
            end_date="now"
        ),                                                  # transaction_date
        random.choice(STATUSES)                             # status
    )

# =========================
# MAIN
# =========================
def main():

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    insert_sql = """
    INSERT INTO sales.transactions (
        customer_id,
        product_id,
        quantity,
        amount,
        transaction_date,
        status
    )
    VALUES %s
    """

    for batch_start in range(0, TOTAL_ROWS, BATCH_SIZE):

        rows = [generate_row() for _ in range(BATCH_SIZE)]

        execute_values(
            cur,
            insert_sql,
            rows
        )

        conn.commit()

        print(
            f"Inserted {min(batch_start + BATCH_SIZE, TOTAL_ROWS):,}"
            f" / {TOTAL_ROWS:,}"
        )

    cur.close()
    conn.close()

    print("✅ Finished inserting 100,000 rows.")

if __name__ == "__main__":
    main()