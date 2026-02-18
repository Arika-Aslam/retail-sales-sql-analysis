
-- =====================================================
-- RETAIL SALES ANALYSIS PROJECT
-- =====================================================
-- Author: Arika Muhammad Aslam
-- Background: Applied Mathematics
-- Tools: PostgreSQL
-- Skills: SQL, Window Functions, Feature Engineering
-- Goal: Business Insights & Machine Learning Preparation
-- =====================================================

DROP TABLE IF EXISTS retail_sales;

CREATE TABLE retail_sales (
row_id TEXT,
order_id TEXT,
order_date TEXT,
ship_date TEXT,
ship_mode TEXT,
customer_id TEXT,
customer_name TEXT,
segment TEXT,
country TEXT,
city TEXT,
state TEXT,
post_code TEXT,
region TEXT,
product_id TEXT,
category TEXT,
sub_category TEXT,
product_name TEXT,
sales TEXT
);
COPY retail_sales
FROM 'C:\Program Files\PostgreSQL\18\data\train.csv'
DELIMITER ','
CSV HEADER;
SELECT COUNT(*) FROM retail_sales;

-- Data Cleaning

BEGIN;

-- new columns with correct data types

ALTER TABLE retail_sales
ADD COLUMN row_id_new INT,
ADD COLUMN order_date_new DATE,
ADD COLUMN ship_date_new DATE,
ADD COLUMN sales_new NUMERIC;


-- Convert and copy data

UPDATE retail_sales
SET
row_id_new = row_id::INT,
order_date_new = TO_DATE(order_date, 'DD/MM/YYYY'),
ship_date_new = TO_DATE(ship_date, 'DD/MM/YYYY'),
sales_new = sales::NUMERIC;


-- Drop old columns

ALTER TABLE retail_sales
DROP COLUMN row_id,
DROP COLUMN order_date,
DROP COLUMN ship_date,
DROP COLUMN sales;


-- Rename new columns to original names

ALTER TABLE retail_sales
RENAME COLUMN row_id_new TO row_id;

ALTER TABLE retail_sales
RENAME COLUMN order_date_new TO order_date;

ALTER TABLE retail_sales
RENAME COLUMN ship_date_new TO ship_date;

ALTER TABLE retail_sales
RENAME COLUMN sales_new TO sales;


COMMIT;

-- PERFORMANCE OPTIMIZATION: INDEXES
CREATE INDEX idx_retail_sales_order_date
ON retail_sales(order_date);

CREATE INDEX idx_retail_sales_customer_id
ON retail_sales(customer_id);

CREATE INDEX idx_retail_sales_product_id
ON retail_sales(product_id);

CREATE INDEX idx_retail_sales_region
ON retail_sales(region);

CREATE INDEX idx_retail_sales_order_id
ON retail_sales(order_id);

--Checking total rows

SELECT COUNT(*) 
FROM retail_sales;

SELECT *
FROM retail_sales
WHERE sales IS NOT NULL
LIMIT 10;

-- BUSINESS QUESTION:
-- Which region generates the highest total revenue?

SELECT region, SUM(sales) AS Total_sales
FROM retail_sales 
GROUP BY region;

-- BUSINESS QUESTION:
-- Who are the top 10 customers by total revenue generated?
SELECT 
customer_id,
SUM(sales) AS total_spent,
RANK() OVER (ORDER BY SUM(sales) DESC) AS rank
FROM retail_sales
GROUP BY customer_id
LIMIT 10;

-- BUSINESS QUESTION:
-- What are the top 10 highest-revenue products?
SELECT product_name, SUM(sales) AS Total_sales
FROM retail_sales 
GROUP BY product_name
ORDER BY Total_sales DESC
LIMIT 10;

-- BUSINESS QUESTION:
-- How has the business revenue grown over time annually?
SELECT 
DATE_PART('year', order_date) AS year,
SUM(sales) AS yearly_sales
FROM retail_sales
GROUP BY year
ORDER BY year;

-- BUSINESS QUESTION:
-- What are the monthly sales trends and seasonality patterns?
SELECT DATE_TRUNC('month', order_date) AS month,
SUM(sales) AS monthly_sales
FROM retail_sales
GROUP BY month
ORDER BY month;

-- BUSINESS QUESTION:
-- Which product categories contribute the most to overall revenue?
SELECT category, SUM(SALES) AS revenue,
ROUND(
100*SUM(sales)/SUM(SUM(sales)) OVER(), 2) as percentage
FROM retail_sales
GROUP BY category
ORDER BY revenue DESC;

--BUSINESS QUESTION:
-- What is the cumulative revenue growth over time?
SELECT order_date, SUM(sales) OVER (ORDER BY order_date) AS running_sales
FROM retail_sales;

-- BUSINESS QUESTION:
-- What is the average time gap between customer purchases?
WITH customer_orders AS (
	SELECT customer_id, order_date, 
	LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)
	AS previous_order
	FROM retail_sales
)

SELECT customer_id, AVG(order_date - Previous_order) AS avg_orders_gap
FROM customer_orders
WHERE previous_order IS NOT NULL
GROUP BY customer_id
ORDER BY avg_orders_gap;

-- BUSINESS QUESTION:
-- Are there unusual spikes or drops in daily sales?
WITH daily_sales AS (
SELECT order_date, SUM(sales) AS total_sales
FROM retail_sales
GROUP BY order_date
)

SELECT order_date, total_sales, AVG(total_sales) OVER (
ORDER BY order_date ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
) AS rolling_avg
FROM daily_sales;

-- BUSINESS QUESTION:
-- Which products are frequently purchased together?
SELECT a.product_name, b.product_name, 
COUNT(*) AS times_bought_together
FROM retail_sales AS a
JOIN retail_sales AS b
ON a.order_id = b.order_id
AND a.product_id < b.product_id
GROUP BY 
a.product_name,
b.product_name
HAVING COUNT(*) > 5
ORDER BY times_bought_together DESC;

-- BUSINESS QUESTION:
-- Is there a statistically significant difference between customer groups?
WITH experiment_groups AS (
SELECT customer_id, 
CASE WHEN MOD(ABS(HASHTEXT(customer_id)), 2) = 0
	THEN 'A'
	ELSE 'B'
	END AS test_group
FROM retail_sales
GROUP BY customer_id
),
customer_revenue AS (
	 SELECT 
	     r.customer_id, 
		 e.test_group,
		 SUM(r.sales) AS customer_total
		 FROM retail_sales AS r
		 INNER JOIN experiment_groups AS e
		 	ON r.customer_id = e.customer_id
		 GROUP BY r.customer_id, e.test_group
)

SELECT 
	test_group, 
	COUNT(*) AS customers, 
	AVG(customer_total) AS avg_customer_revenue, 
	STDDEV(customer_total) AS stddev_customer_revenue
	FROM customer_revenue
	GROUP BY test_group;

-- BUSINESS QUESTION:
-- Can we build customer features for machine learning and segmentation?
DROP MATERIALIZED VIEW IF EXISTS customer_features;
DROP TABLE IF EXISTS customer_features;
CREATE TABLE customer_features AS 
SELECT 
	customer_id,
	COUNT(DISTINCT order_id) AS total_orders,
	SUM(sales) AS total_spend,
	AVG(sales) AS avg_order_value,
	MAX(order_date)-MIN(order_date) AS tenure_days,
	STDDEV(sales) AS spend_volatility
FROM retail_sales
GROUP BY customer_id;
SELECT *
FROM customer_features
LIMIT 5;

