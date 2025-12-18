#!/bin/bash
set -e

echo "Starting database initialization..."

# Metastore və Source DB yaradırıq
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE metastore;
    CREATE DATABASE source_db;
    
    GRANT ALL PRIVILEGES ON DATABASE metastore TO airflow;
    GRANT ALL PRIVILEGES ON DATABASE source_db TO airflow;
EOSQL

echo "Databases created: metastore, source_db"

# Source DB-də sample data yaradırıq
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "source_db" <<-EOSQL
    -- Sales table
    CREATE TABLE IF NOT EXISTS sales (
        id SERIAL PRIMARY KEY,
        order_date TIMESTAMP NOT NULL,
        customer_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Customers table
    CREATE TABLE IF NOT EXISTS customers (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100),
        country VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Products table
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        category VARCHAR(50),
        unit_price DECIMAL(10,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Sample data - Customers
    INSERT INTO customers (name, email, country) VALUES
    ('Ali Məmmədov', 'ali@example.com', 'Azerbaijan'),
    ('Leyla İbrahimova', 'leyla@example.com', 'Azerbaijan'),
    ('John Smith', 'john@example.com', 'USA'),
    ('Maria Garcia', 'maria@example.com', 'Spain'),
    ('Wei Zhang', 'wei@example.com', 'China')
    ON CONFLICT DO NOTHING;

    -- Sample data - Products
    INSERT INTO products (name, category, unit_price) VALUES
    ('Laptop', 'Electronics', 1200.00),
    ('Mouse', 'Electronics', 25.00),
    ('Keyboard', 'Electronics', 75.00),
    ('Monitor', 'Electronics', 300.00),
    ('Desk Chair', 'Furniture', 250.00),
    ('Desk Lamp', 'Furniture', 45.00),
    ('USB Cable', 'Electronics', 10.00),
    ('Notebook', 'Stationery', 5.00),
    ('Pen Set', 'Stationery', 15.00),
    ('Coffee Mug', 'Kitchen', 12.00)
    ON CONFLICT DO NOTHING;

    -- Sample data - Sales (100 random records)
    INSERT INTO sales (order_date, customer_id, product_id, quantity, price)
    SELECT 
        CURRENT_DATE - (random() * 90)::integer * interval '1 day',
        (random() * 4 + 1)::integer,
        (random() * 9 + 1)::integer,
        (random() * 10 + 1)::integer,
        (random() * 1000 + 50)::numeric(10,2)
    FROM generate_series(1, 100)
    ON CONFLICT DO NOTHING;

    -- Index-lər
    CREATE INDEX IF NOT EXISTS idx_sales_order_date ON sales(order_date);
    CREATE INDEX IF NOT EXISTS idx_sales_customer_id ON sales(customer_id);
    CREATE INDEX IF NOT EXISTS idx_sales_product_id ON sales(product_id);
EOSQL

echo "Sample data created successfully!"
echo "Tables: sales (100 records), customers (5 records), products (10 records)"
echo "Database initialization completed!"