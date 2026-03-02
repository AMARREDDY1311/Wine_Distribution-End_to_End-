-- ============================================================
-- SCRIPT  : quality_checks_silver.sql
-- PURPOSE : Validate Silver layer data after EXEC silver.load_silver
--           Run these checks manually in SSMS to spot issues.
--           Checks mirror the same categories used in the reference
--           project: NULLs, duplicates, invalid ranges, bad dates,
--           cross-field consistency.
-- ============================================================

USE BresCome_DW;
GO

PRINT '============================================================';
PRINT 'SILVER LAYER — DATA QUALITY CHECKS';
PRINT '============================================================';

-- ════════════════════════════════════════════════════════════
-- SECTION 1 : NULL / MISSING KEY CHECKS
-- Expected result: 0 rows for every query
-- ════════════════════════════════════════════════════════════

-- 1a. Reps with NULL rep_id (should never happen)
PRINT '1a. NULL rep_id in silver.sales_reps:';
SELECT COUNT(*) AS null_rep_ids
FROM silver.sales_reps
WHERE rep_id IS NULL;

-- 1b. Transactions with NULL txn_date
PRINT '1b. NULL txn_date in silver.sales_transactions:';
SELECT COUNT(*) AS null_dates
FROM silver.sales_transactions
WHERE txn_date IS NULL;

-- 1c. Transactions with NULL revenue
PRINT '1c. NULL or zero revenue_usd in silver.sales_transactions:';
SELECT COUNT(*) AS bad_revenue
FROM silver.sales_transactions
WHERE revenue_usd IS NULL OR revenue_usd <= 0;

-- 1d. Products with NULL price
PRINT '1d. NULL price_per_case_usd in silver.products:';
SELECT COUNT(*) AS null_prices
FROM silver.products
WHERE price_per_case_usd IS NULL;

-- 1e. Programs with NULL goal_value or bonus
PRINT '1e. NULL goal_value or bonus_usd in silver.supplier_programs:';
SELECT COUNT(*) AS null_program_values
FROM silver.supplier_programs
WHERE goal_value IS NULL OR bonus_usd IS NULL;


-- ════════════════════════════════════════════════════════════
-- SECTION 2 : DUPLICATE KEY CHECKS
-- Expected result: 0 rows for every query
-- ════════════════════════════════════════════════════════════

-- 2a. Duplicate rep_ids
PRINT '2a. Duplicate rep_ids in silver.sales_reps:';
SELECT rep_id, COUNT(*) AS cnt
FROM silver.sales_reps
GROUP BY rep_id
HAVING COUNT(*) > 1;

-- 2b. Duplicate txn_ids
PRINT '2b. Duplicate txn_ids in silver.sales_transactions:';
SELECT txn_id, COUNT(*) AS cnt
FROM silver.sales_transactions
GROUP BY txn_id
HAVING COUNT(*) > 1;

-- 2c. Duplicate sku_ids
PRINT '2c. Duplicate sku_ids in silver.products:';
SELECT sku_id, COUNT(*) AS cnt
FROM silver.products
GROUP BY sku_id
HAVING COUNT(*) > 1;

-- 2d. Duplicate rep + month combos in quota table
PRINT '2d. Duplicate rep_id + month_year in silver.rep_quota_actuals:';
SELECT rep_id, month_year, COUNT(*) AS cnt
FROM silver.rep_quota_actuals
GROUP BY rep_id, month_year
HAVING COUNT(*) > 1;


-- ════════════════════════════════════════════════════════════
-- SECTION 3 : INVALID DATE CHECKS
-- ════════════════════════════════════════════════════════════

-- 3a. Transaction dates outside expected range (2024-2025)
PRINT '3a. Transactions outside 2024-01-01 to 2025-12-31:';
SELECT COUNT(*) AS out_of_range
FROM silver.sales_transactions
WHERE txn_date < '2024-01-01'
   OR txn_date > '2025-12-31';

-- 3b. Programs where start_date is after end_date
PRINT '3b. Programs with start_date > end_date:';
SELECT program_id, start_date, end_date
FROM silver.supplier_programs
WHERE start_date > end_date;

-- 3c. Reps with hire_date in the future
PRINT '3c. Reps hired in the future:';
SELECT rep_id, rep_name, hire_date
FROM silver.sales_reps
WHERE hire_date > CAST(GETDATE() AS DATE);


-- ════════════════════════════════════════════════════════════
-- SECTION 4 : STANDARDIZATION CHECKS
-- Expected result: only valid category values returned
-- ════════════════════════════════════════════════════════════

-- 4a. Channel values — should only be 'On-Premise' or 'Off-Premise'
PRINT '4a. Distinct channel values in silver.accounts:';
SELECT DISTINCT channel, COUNT(*) AS cnt
FROM silver.accounts
GROUP BY channel
ORDER BY channel;

-- 4b. Supplier category values
PRINT '4b. Distinct supplier categories:';
SELECT DISTINCT category, COUNT(*) AS cnt
FROM silver.suppliers
GROUP BY category;

-- 4c. Performance flag values in quota table
PRINT '4c. Distinct performance_flag values:';
SELECT DISTINCT performance_flag, COUNT(*) AS cnt
FROM silver.rep_quota_actuals
GROUP BY performance_flag;

-- 4d. Program attainment status values
PRINT '4d. Distinct status values in silver.program_attainments:';
SELECT DISTINCT status, COUNT(*) AS cnt
FROM silver.program_attainments
GROUP BY status;


-- ════════════════════════════════════════════════════════════
-- SECTION 5 : CROSS-FIELD CONSISTENCY
-- ════════════════════════════════════════════════════════════

-- 5a. Transactions where is_valid = 0 (flagged as bad data)
PRINT '5a. Invalid transactions (is_valid = 0):';
SELECT txn_id, txn_date, cases_sold, revenue_usd, is_valid
FROM silver.sales_transactions
WHERE is_valid = 0
ORDER BY txn_id;

-- 5b. Transactions where attainment_pct doesn't match actual/goal ratio
--     (flag discrepancy > 1% as potential issue)
PRINT '5b. Attainments where pct doesnt match actual/goal (>1% gap):';
SELECT
    attainment_id,
    goal_value,
    actual_value,
    attainment_pct                                      AS stored_pct,
    ROUND(actual_value / NULLIF(goal_value,0) * 100, 2) AS calc_pct,
    ABS(attainment_pct - ROUND(actual_value / NULLIF(goal_value,0) * 100, 2)) AS gap
FROM silver.program_attainments
WHERE ABS(attainment_pct - ROUND(actual_value / NULLIF(goal_value,0) * 100, 2)) > 1
ORDER BY gap DESC;

-- 5c. Orphan transactions — rep_id in transactions not in sales_reps
PRINT '5c. Transactions with rep_id not found in silver.sales_reps:';
SELECT DISTINCT st.rep_id
FROM silver.sales_transactions st
LEFT JOIN silver.sales_reps sr ON st.rep_id = sr.rep_id
WHERE sr.rep_id IS NULL;

-- 5d. Orphan accounts — rep_id in accounts not in sales_reps
PRINT '5d. Accounts with rep_id not found in silver.sales_reps:';
SELECT DISTINCT a.rep_id, a.account_name
FROM silver.accounts a
LEFT JOIN silver.sales_reps sr ON a.rep_id = sr.rep_id
WHERE sr.rep_id IS NULL;


-- ════════════════════════════════════════════════════════════
-- SECTION 6 : ROW COUNT VALIDATION
-- Compare expected vs actual row counts per table
-- ════════════════════════════════════════════════════════════

PRINT '';
PRINT '════ ROW COUNT SUMMARY ════';
SELECT 'silver.sales_reps'         AS table_name, COUNT(*) AS row_count FROM silver.sales_reps         UNION ALL
SELECT 'silver.suppliers',                         COUNT(*) FROM silver.suppliers                       UNION ALL
SELECT 'silver.products',                          COUNT(*) FROM silver.products                        UNION ALL
SELECT 'silver.accounts',                          COUNT(*) FROM silver.accounts                        UNION ALL
SELECT 'silver.supplier_programs',                 COUNT(*) FROM silver.supplier_programs               UNION ALL
SELECT 'silver.sales_transactions',                COUNT(*) FROM silver.sales_transactions              UNION ALL
SELECT 'silver.rep_quota_actuals',                 COUNT(*) FROM silver.rep_quota_actuals               UNION ALL
SELECT 'silver.program_attainments',               COUNT(*) FROM silver.program_attainments;

PRINT '============================================================';
PRINT 'Quality checks complete. All zero-result queries = PASS.';
PRINT '============================================================';
