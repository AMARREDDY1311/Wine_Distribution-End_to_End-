-- ============================================================
-- SCRIPT  : ddl_gold.sql
-- PURPOSE : Fix gold.dim_date — replace recursive CTE (which
--           requires OPTION MAXRECURSION and cannot be used
--           inside a VIEW) with a cross-join tally approach.
--           Also re-runs the final summary row count query.
-- RUN     : After ddl_gold.sql has been executed
-- ============================================================
USE BresCome_DW;
GO

-- ============================================================
-- DIMENSIONS
-- ============================================================

IF OBJECT_ID('gold.fact_attainment','V') IS NOT NULL DROP VIEW gold.fact_attainment;
IF OBJECT_ID('gold.fact_quota','V')      IS NOT NULL DROP VIEW gold.fact_quota;
IF OBJECT_ID('gold.fact_sales','V')      IS NOT NULL DROP VIEW gold.fact_sales;
IF OBJECT_ID('gold.dim_date','V')        IS NOT NULL DROP VIEW gold.dim_date;
IF OBJECT_ID('gold.dim_programs','V')    IS NOT NULL DROP VIEW gold.dim_programs;
IF OBJECT_ID('gold.dim_suppliers','V')   IS NOT NULL DROP VIEW gold.dim_suppliers;
IF OBJECT_ID('gold.dim_products','V')    IS NOT NULL DROP VIEW gold.dim_products;
IF OBJECT_ID('gold.dim_accounts','V')    IS NOT NULL DROP VIEW gold.dim_accounts;
IF OBJECT_ID('gold.dim_reps','V')        IS NOT NULL DROP VIEW gold.dim_reps;
GO

CREATE VIEW gold.dim_reps AS
SELECT ROW_NUMBER() OVER (ORDER BY rep_id) AS rep_key, rep_id AS rep_src_id,
rep_name, division, channel, hire_date,
DATEDIFF(YEAR,hire_date,GETDATE()) AS years_tenure, email
FROM silver.sales_reps;
GO

CREATE VIEW gold.dim_accounts AS
SELECT ROW_NUMBER() OVER (ORDER BY account_id) AS account_key,
account_id AS account_src_id, account_name, account_type, channel,
town, state, division, rep_id AS rep_src_id, is_active,
CASE is_active WHEN 1 THEN 'Active' ELSE 'Inactive' END AS account_status
FROM silver.accounts;
GO

CREATE VIEW gold.dim_products AS
SELECT ROW_NUMBER() OVER (ORDER BY sku_id) AS product_key,
sku_id AS sku_src_id, supplier_id AS supplier_src_id, product_name,
category, sub_category, size_ml, price_per_case_usd,
CASE WHEN price_per_case_usd < 100 THEN 'Value'
     WHEN price_per_case_usd < 200 THEN 'Mid-Range'
     WHEN price_per_case_usd < 350 THEN 'Premium'
     ELSE 'Ultra-Premium' END AS price_tier
FROM silver.products;
GO

CREATE VIEW gold.dim_suppliers AS
SELECT ROW_NUMBER() OVER (ORDER BY supplier_id) AS supplier_key,
supplier_id AS supplier_src_id, supplier_name, category, country,
CASE WHEN UPPER(country)='USA' THEN 'Domestic' ELSE 'Import' END AS origin_type
FROM silver.suppliers;
GO

CREATE VIEW gold.dim_programs AS
SELECT ROW_NUMBER() OVER (ORDER BY program_id) AS program_key,
program_id AS program_src_id, supplier_id AS supplier_src_id,
quarter, start_date, end_date, program_type, goal_value, bonus_usd, division,
DATEDIFF(DAY,start_date,end_date)+1 AS program_duration_days
FROM silver.supplier_programs;
GO

-- dim_date: cross join tally (no recursive CTE = no OPTION MAXRECURSION needed)
CREATE VIEW gold.dim_date AS
WITH numbers AS (
    SELECT TOP 730 ROW_NUMBER() OVER (ORDER BY (SELECT NULL))-1 AS n
    FROM sys.objects a CROSS JOIN sys.objects b
),
date_spine AS (
    SELECT DATEADD(DAY,n,CAST('2024-01-01' AS DATE)) AS full_date FROM numbers
)
SELECT full_date,
CAST(FORMAT(full_date,'yyyyMMdd') AS INT) AS date_key,
YEAR(full_date) AS year, MONTH(full_date) AS month_num,
DATENAME(MONTH,full_date) AS month_name,
LEFT(DATENAME(MONTH,full_date),3) AS month_short,
FORMAT(full_date,'yyyy-MM') AS year_month,
DAY(full_date) AS day_of_month,
DATEPART(WEEKDAY,full_date) AS day_of_week_num,
DATENAME(WEEKDAY,full_date) AS day_of_week_name,
DATEPART(QUARTER,full_date) AS quarter_num,
'Q'+CAST(DATEPART(QUARTER,full_date) AS NVARCHAR) AS quarter_label,
FORMAT(full_date,'yyyy')+'-Q'+CAST(DATEPART(QUARTER,full_date) AS NVARCHAR) AS year_quarter,
CASE WHEN DATEPART(WEEKDAY,full_date) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
CASE WHEN MONTH(full_date) IN (11,12) THEN 'Holiday Season'
     WHEN MONTH(full_date) IN (6,7,8)  THEN 'Summer Peak'
     ELSE 'Standard' END AS season
FROM date_spine;
GO

-- ============================================================
-- FACTS
-- ============================================================

CREATE VIEW gold.fact_sales AS
SELECT st.txn_id, dr.rep_key, da.account_key, dp.product_key, ds.supplier_key,
st.txn_date AS order_date, st.cases_sold, st.discount_pct, st.revenue_usd,
ROUND(st.revenue_usd/NULLIF(st.cases_sold,0),2) AS revenue_per_case,
st.cases_sold*ISNULL(pr.price_per_case_usd,0) AS gross_revenue_usd,
ROUND(st.cases_sold*ISNULL(pr.price_per_case_usd,0)*(st.discount_pct/100.0),2) AS discount_usd,
ROUND(st.revenue_usd*0.30,2) AS estimated_margin_usd,
st.channel, st.is_valid, st.dwh_create_date
FROM silver.sales_transactions st
LEFT JOIN gold.dim_reps      dr  ON st.rep_id      = dr.rep_src_id
LEFT JOIN gold.dim_accounts  da  ON st.account_id  = da.account_src_id
LEFT JOIN gold.dim_products  dp  ON st.sku_id      = dp.sku_src_id
LEFT JOIN silver.products    pr  ON st.sku_id      = pr.sku_id
LEFT JOIN gold.dim_suppliers ds  ON st.supplier_id = ds.supplier_src_id
WHERE st.is_valid = 1;
GO

CREATE VIEW gold.fact_quota AS
SELECT dr.rep_key, rq.month_year, rq.quota_cases, rq.actual_cases,
rq.attainment_pct, rq.quota_revenue_usd, rq.actual_revenue_usd,
rq.actual_cases - rq.quota_cases AS cases_variance,
rq.actual_revenue_usd - rq.quota_revenue_usd AS revenue_variance_usd,
SUM(rq.actual_cases) OVER (PARTITION BY rq.rep_id,LEFT(rq.month_year,4) ORDER BY rq.month_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ytd_actual_cases,
SUM(rq.quota_cases)  OVER (PARTITION BY rq.rep_id,LEFT(rq.month_year,4) ORDER BY rq.month_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ytd_quota_cases,
rq.performance_flag, dr.rep_name, dr.division, dr.channel, rq.dwh_create_date
FROM silver.rep_quota_actuals rq
LEFT JOIN gold.dim_reps dr ON rq.rep_id = dr.rep_src_id;
GO

CREATE VIEW gold.fact_attainment AS
SELECT dr.rep_key, ds.supplier_key, dgp.program_key, pa.quarter,
pa.goal_value, pa.actual_value, pa.attainment_pct, pa.bonus_earned_usd,
pa.goal_value - pa.actual_value AS gap_to_goal,
CASE WHEN pa.attainment_pct >= 100 THEN 0 ELSE ROUND(dgp.bonus_usd - pa.bonus_earned_usd,2) END AS bonus_missed_usd,
pa.status,
CASE pa.status WHEN 'Achieved' THEN 1 WHEN 'At Risk' THEN 2 WHEN 'Behind' THEN 3 ELSE 9 END AS status_sort_order,
pa.program_type, pa.division, dr.rep_name, dr.channel,
ds.supplier_name, ds.category AS supplier_category, pa.dwh_create_date
FROM silver.program_attainments pa
LEFT JOIN gold.dim_reps      dr  ON pa.rep_id      = dr.rep_src_id
LEFT JOIN gold.dim_suppliers ds  ON pa.supplier_id = ds.supplier_src_id
LEFT JOIN gold.dim_programs  dgp ON pa.program_id  = dgp.program_src_id;
GO

-- ============================================================
-- SUMMARY
-- ============================================================
SELECT 'gold.dim_reps'       AS object_name, COUNT(*) AS row_count FROM gold.dim_reps       UNION ALL
SELECT 'gold.dim_accounts',                  COUNT(*) FROM gold.dim_accounts                 UNION ALL
SELECT 'gold.dim_products',                  COUNT(*) FROM gold.dim_products                 UNION ALL
SELECT 'gold.dim_suppliers',                 COUNT(*) FROM gold.dim_suppliers                UNION ALL
SELECT 'gold.dim_programs',                  COUNT(*) FROM gold.dim_programs                 UNION ALL
SELECT 'gold.dim_date',                      COUNT(*) FROM gold.dim_date                     UNION ALL
SELECT 'gold.fact_sales',                    COUNT(*) FROM gold.fact_sales                   UNION ALL
SELECT 'gold.fact_quota',                    COUNT(*) FROM gold.fact_quota                   UNION ALL
SELECT 'gold.fact_attainment',               COUNT(*) FROM gold.fact_attainment;
