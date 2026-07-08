import psycopg2
import random
from psycopg2.extras import execute_batch

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

# =========================
# SETTINGS
# =========================
TOTAL_UPDATES = 1000
BATCH_SIZE = 100

STATUSES = [
    "Completed",
    "Pending",
    "Cancelled"
]

# =========================
# MAIN
# =========================
def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Mövcud transaction id-ləri götür
    cur.execute("SELECT id FROM sales.transactions")
    ids = [row[0] for row in cur.fetchall()]

    if not ids:
        print("Table is empty.")
        return

    update_sql = """
        UPDATE sales.transactions
        SET
            quantity = %s,
            amount = %s,
            status = %s
        WHERE id = %s
    """

    for batch_start in range(0, TOTAL_UPDATES, BATCH_SIZE):

        batch = []

        for _ in range(BATCH_SIZE):
            row_id = random.choice(ids)

            quantity = random.randint(1, 20)
            price = round(random.uniform(10, 1000), 2)
            amount = round(quantity * price, 2)
            status = random.choice(STATUSES)

            batch.append((
                quantity,
                amount,
                status,
                row_id
            ))

        execute_batch(cur, update_sql, batch)

        conn.commit()

        print(
            f"Updated {min(batch_start + BATCH_SIZE, TOTAL_UPDATES):,}"
            f" / {TOTAL_UPDATES:,}"
        )

    cur.close()
    conn.close()

    print("✅ Update completed.")

if __name__ == "__main__":
    main()