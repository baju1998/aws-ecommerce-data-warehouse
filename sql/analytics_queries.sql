-- Total merchandise sales by year and month

SELECT
    d.year,
    d.month,
    SUM(f.item_price) AS total_sales,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(*) AS total_items_sold
FROM fact_sales AS f
JOIN dim_date AS d
    ON f.date_key = d.date_key
GROUP BY
    d.year,
    d.month
ORDER BY
    d.year,
    d.month;





-- Business Question 1:
-- How do monthly merchandise sales,
-- shipping charges, and order counts change over time?


    SELECT
    d.year,
    d.month,
    ROUND(SUM(f.item_price), 2) AS product_revenue,
    ROUND(SUM(f.freight_value), 2) AS shipping_revenue,
    ROUND(SUM(f.total_item_amount), 2) AS total_sales,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM ecommerce_dw.fact_sales AS f
INNER JOIN ecommerce_dw.dim_date AS d
    ON f.date_key = d.date_key
GROUP BY
    d.year,
    d.month
ORDER BY
    d.year,
    d.month;



--This query returns the top 10 product categories ranked by merchandise revenue, excluding shipping.
-- Business Question 2:
-- Which product categories generate the most revenue?

WITH category_sales AS (
    SELECT
        COALESCE(
            p.product_category_name,
            'Unknown'
        ) AS product_category,
        COUNT(*) AS items_sold,
        ROUND(SUM(f.item_price), 2) AS product_revenue
    FROM ecommerce_dw.fact_sales AS f
    INNER JOIN ecommerce_dw.dim_product AS p
        ON f.product_key = p.product_key
    GROUP BY
        COALESCE(p.product_category_name, 'Unknown')
)

SELECT
    product_category,
    items_sold,
    product_revenue,
    DENSE_RANK() OVER (
        ORDER BY product_revenue DESC
    ) AS revenue_rank
FROM category_sales
ORDER BY revenue_rank, product_category
LIMIT 10;






-- Business Question 3:
-- Which sellers generate the most merchandise revenue?
-- How many distinct orders does each seller fulfill?

WITH seller_sales AS (
    SELECT
        s.seller_key,
        s.seller_city,
        s.seller_state,
        COUNT(DISTINCT f.order_id) AS total_orders,
        COUNT(*) AS items_sold,
        ROUND(SUM(f.item_price), 2) AS product_revenue
    FROM ecommerce_dw.fact_sales AS f
    INNER JOIN ecommerce_dw.dim_seller AS s
        ON f.seller_key = s.seller_key
    GROUP BY
        s.seller_key,
        s.seller_city,
        s.seller_state
)

SELECT
    seller_key,
    seller_city,
    seller_state,
    total_orders,
    items_sold,
    product_revenue,
    DENSE_RANK() OVER (
        ORDER BY product_revenue DESC
    ) AS revenue_rank
FROM seller_sales
ORDER BY revenue_rank, seller_key
LIMIT 10;







-- Business Question 4:
-- Which customer states generate the most merchandise revenue?
-- What is the average order value in each state?


WITH state_sales AS (
    SELECT
        c.customer_state,
        COUNT(DISTINCT f.order_id) AS total_orders,
        COUNT(*) AS items_sold,
        ROUND(SUM(f.item_price), 2) AS product_revenue
    FROM ecommerce_dw.fact_sales AS f
    INNER JOIN ecommerce_dw.dim_customer AS c
        ON f.customer_key = c.customer_key
    GROUP BY
        c.customer_state
)

SELECT
    customer_state,
    total_orders,
    items_sold,
    product_revenue,
    ROUND(
        product_revenue / NULLIF(total_orders, 0),
        2
    ) AS average_order_value,
    DENSE_RANK() OVER (
        ORDER BY product_revenue DESC
    ) AS revenue_rank
FROM state_sales
ORDER BY revenue_rank, customer_state
LIMIT 10;