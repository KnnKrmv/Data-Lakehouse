"""
dags/common_datasets.py

Bronze -> Silver -> Gold runtime asılılığı üçün Airflow Dataset-ləri.
Bütün 3 DAG bu faylı import edir.
"""
from airflow.datasets import Dataset

BRONZE_TRANSACTIONS_DS = Dataset("iceberg://bronze.sales.transactions")
SILVER_TRANSACTIONS_DS = Dataset("iceberg://silver.sales.transactions")
GOLD_TRANSACTIONS_DS = Dataset("iceberg://gold.sales.transactions")



BRONZE_READY = Dataset("iceberg://bronze_batch/ready")
SILVER_READY = Dataset("iceberg://silver_batch/ready")