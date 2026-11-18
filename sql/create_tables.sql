CREATE TABLE IF NOT EXISTS dim_customers(
    customer_key VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);

-- Product dimension
CREATE TABLE IF NOT EXISTS dim_product (
    product_key VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_weight_g INTEGER
);

-- Seller dimension
CREATE TABLE IF NOT EXISTS dim_seller (
    seller_key VARCHAR(50) PRIMARY KEY,
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);

-- Date dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER
);

-- Sales fact table
CREATE TABLE IF NOT EXISTS fact_sales (
    order_id VARCHAR(50),
    order_item_id INTEGER,
    product_key VARCHAR(50) REFERENCES dim_product(product_key),
    customer_key VARCHAR(50) REFERENCES dim_customer(customer_key),
    seller_key VARCHAR(50) REFERENCES dim_seller(seller_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    item_price DECIMAL(12,2),
    freight_value DECIMAL(12,2),

    PRIMARY KEY (order_id, order_item_id)
);