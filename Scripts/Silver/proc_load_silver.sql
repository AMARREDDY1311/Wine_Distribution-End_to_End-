-- ============================================================
-- SCRIPT  : proc_load_silver.sql
-- PURPOSE : ETL stored procedure — Layer 1 transforms.
--           Reads from bronze schema, applies:
--             1. Type casting   (NVARCHAR → INT / DATE / DECIMAL / BIT)
--             2. Trimming       (LTRIM / RTRIM on all strings)
--             3. Standardization (channel values, status labels)
--             4. Deduplication  (ROW_NUMBER() keeps latest per key)
--             5. Null handling  (COALESCE with safe defaults)
--             6. Derived columns (performance_flag, is_valid)
--             7. Basic validation flagging (revenue, date checks)
--           Strategy : TRUNCATE silver table then INSERT (full reload)
-- USAGE   : EXEC silver.load_silver;
-- ============================================================

USE BresCome_DW;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time  DATETIME2;
    DECLARE @end_time    DATETIME2;
    DECLARE @row_count   INT;

    PRINT '============================================================';
    PRINT 'SILVER LAYER LOAD — Brescome Barton Data Warehouse';
    PRINT 'Start time : ' + CONVERT(NVARCHAR, GETDATE(), 120);
    PRINT '============================================================';

    -- ────────────────────────────────────────────────────────────
    -- TABLE 1 : silver.sales_reps
    -- Transforms:
    --   • rep_id   → CAST to INT
    --   • hire_date → TRY_CAST to DATE (safe — nulls if bad date)
    --   • rep_name, division, channel, email → LTRIM/RTRIM
    --   • Dedup on rep_id (keep latest)
    -- ────────────────────────────────────────────────────────────
    BEGIN TRY
        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>> Transforming bronze.sales_reps → silver.sales_reps ...';

        TRUNCATE TABLE silver.sales_reps;

        INSERT INTO silver.sales_reps (
            rep_id, rep_name, division, channel, hire_date, email
        )
        SELECT
            CAST(rep_id AS INT),
            LTRIM(RTRIM(rep_name)),
            LTRIM(RTRIM(division)),
            -- Standardize channel to exactly 'On-Premise' or 'Off-Premise'
            CASE
                WHEN LTRIM(RTRIM(UPPER(channel))) = 'ON-PREMISE'  THEN 'On-Premise'
                WHEN LTRIM(RTRIM(UPPER(channel))) = 'OFF-PREMISE' THEN 'Off-Premise'
                ELSE LTRIM(RTRIM(channel))
            END,
            TRY_CAST(hire_date AS DATE),
            LTRIM(RTRIM(LOWER(email)))   -- normalize email to lowercase
        FROM (
            -- Deduplication: if the same rep_id appears more than once, keep the latest row
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY rep_id
                    ORDER BY dwh_create_date DESC
                ) AS rn
            FROM bronze.sales_reps
            WHERE rep_id IS NOT NULL
              AND LTRIM(RTRIM(rep_id)) != ''
        ) dedup
        WHERE rn = 1;

        SET @row_count = @@ROWCOUNT;
        SET @end_time  = GETDATE();
        PRINT '   ✅ ' + CAST(@row_count AS NVARCHAR) + ' rows loaded | '
              + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' ms';
    END TRY
    BEGIN CATCH
        PRINT '   ❌ ERROR: ' + ERROR_MESSAGE();
        THROW;
    END CATCH;

    -- ────────────────────────────────────────────────────────────
    -- TABLE 2 : silver.suppliers
    -- Transforms:
    --   • supplier_id → INT
    --   • category    → standardized title case
    --   • Dedup on supplier_id
    -- ────────────────────────────────────────────────────────────
    BEGIN TRY
        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>> Transforming bronze.suppliers → silver.suppliers ...';

        TRUNCATE TABLE silver.suppliers;

        INSERT INTO silver.suppliers (
            supplier_id, supplier_name, category, country
        )
        SELECT
            CAST(supplier_id AS INT),
            LTRIM(RTRIM(supplier_name)),
            -- Normalize category labels
            CASE
                WHEN LTRIM(RTRIM(UPPER(category))) = 'WINE'            THEN 'Wine'
                WHEN LTRIM(RTRIM(UPPER(category))) = 'SPIRITS'         THEN 'Spirits'
                WHEN LTRIM(RTRIM(UPPER(category))) LIKE '%WINE%SPIRIT%' THEN 'Wine & Spirits'
                ELSE LTRIM(RTRIM(category))
            END,
            LTRIM(RTRIM(country))
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY supplier_id
                    ORDER BY dwh_create_date DESC
                ) AS rn
            FROM bronze.suppliers
            WHERE supplier_id IS NOT NULL
        ) dedup
        WHERE rn = 1;

        SET @row_count = @@ROWCOUNT;
        SET @end_time  = GETDATE();
        PRINT '   ✅ ' + CAST(@row_count AS NVARCHAR) + ' rows loaded | '
              + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' ms';
    END TRY
    BEGIN CATCH
        PRINT '   ❌ ERROR: ' + ERROR_MESSAGE();
        THROW;
    END CATCH;

    -- ────────────────────────────────────────────────────────────
    -- TABLE 3 : silver.products
    -- Transforms:
    --   • sku_id, supplier_id → INT
    --   • size_ml            → INT
    --   • price_per_case_usd → DECIMAL(10,2)
    --   • category / sub_category → trimmed
    --   • Dedup on sku_id
    -- ────────────────────────────────────────────────────────────
    BEGIN TRY
        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>> Transforming bronze.products → silver.products ...';

        TRUNCATE TABLE silver.products;

        INSERT INTO silver.products (
            sku_id, product_name, supplier_id, category,
            sub_category, size_ml, price_per_case_usd
        )
        SELECT
            CAST(sku_id AS INT),
            LTRIM(RTRIM(product_name)),
            CAST(supplier_id AS INT),
            LTRIM(RTRIM(category)),
            LTRIM(RTRIM(sub_category)),
            TRY_CAST(size_ml AS INT),
            TRY_CAST(price_per_case_usd AS DECIMAL(10,2))
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY sku_id
                    ORDER BY dwh_create_date DESC
                ) AS rn
            FROM bronze.products
            WHERE sku_id IS NOT NULL
        ) dedup
        WHERE rn = 1;

        SET @row_count = @@ROWCOUNT;
        SET @end_time  = GETDATE();
        PRINT '   ✅ ' + CAST(@row_count AS NVARCHAR) + ' rows loaded | '
              + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' ms';
    END TRY
    BEGIN CATCH
        PRINT '   ❌ ERROR: ' + ERROR_MESSAGE();
        THROW;
    END CATCH;

    -- ────────────────────────────────────────────────────────────
    -- TABLE 4 : silver.accounts
    -- Transforms:
    --   • account_id, rep_id → INT
    --   • active '0'/'1'     → BIT, renamed to is_active
    --   • channel            → standardized
    --   • state              → UPPER
    --   • Dedup on account_id
    -- ────────────────────────────────────────────────────────────
    BEGIN TRY
        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>> Transforming bronze.accounts → silver.accounts ...';

        TRUNCATE TABLE silver.accounts;

        INSERT INTO silver.accounts (
            account_id, account_name, account_type, channel,
            town, state, division, rep_id, is_active
        )
        SELECT
            CAST(account_id AS INT),
            LTRIM(RTRIM(account_name)),
            LTRIM(RTRIM(account_type)),
            CASE
                WHEN LTRIM(RTRIM(UPPER(channel))) = 'ON-PREMISE'  THEN 'On-Premise'
                WHEN LTRIM(RTRIM(UPPER(channel))) = 'OFF-PREMISE' THEN 'Off-Premise'
                ELSE LTRIM(RTRIM(channel))
            END,
            LTRIM(RTRIM(town)),
            UPPER(LTRIM(RTRIM(state))),
            LTRIM(RTRIM(division)),
            CAST(rep_id AS INT),
            CAST(COALESCE(active, '0') AS BIT)   -- default inactive if null
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY account_id
                    ORDER BY dwh_create_date DESC
                ) AS rn
            FROM bronze.accounts
            WHERE account_id IS NOT NULL
        ) dedup
        WHERE rn = 1;

        SET @row_count = @@ROWCOUNT;
        SET @end_time  = GETDATE();
        PRINT '   ✅ ' + CAST(@row_count AS NVARCHAR) + ' rows loaded | '
              + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' ms';
    END TRY
    BEGIN CATCH
        PRINT '   ❌ ERROR: ' + ERROR_MESSAGE();
        THROW;
    END CATCH;

    -- ────────────────────────────────────────────────────────────
    -- TABLE 5 : silver.supplier_programs
    -- Transforms:
    --   • program_id, supplier_id → INT
    --   • start_date, end_date    → DATE
    --   • goal_value, bonus_usd   → DECIMAL
    --   • Validation: reject rows where start_date > end_date
    --   • Dedup on program_id
    -- ────────────────────────────────────────────────────────────
    BEGIN TRY
        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>> Transforming bronze.supplier_programs → silver.supplier_programs ...';

        TRUNCATE TABLE silver.supplier_programs;

        INSERT INTO silver.supplier_programs (
            program_id, supplier_id, quarter, start_date, end_date,
            program_type, goal_value, bonus_usd, division
        )
        SELECT
            CAST(program_id AS INT),
            CAST(supplier_id AS INT),
            LTRIM(RTRIM(quarter)),
            TRY_CAST(start_date AS DATE),
            TRY_CAST(end_date AS DATE),
            LTRIM(RTRIM(program_type)),
            TRY_CAST(goal_value AS DECIMAL(12,2)),
            TRY_CAST(bonus_usd  AS DECIMAL(12,2)),
            LTRIM(RTRIM(division))
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY program_id
                    ORDER BY dwh_create_date DESC
                ) AS rn
            FROM bronze.supplier_programs
            WHERE program_id IS NOT NULL
        ) dedup
        WHERE rn = 1
          -- Data quality gate: do not load programs with invalid date range
          AND TRY_CAST(start_date AS DATE) IS NOT NULL
          AND TRY_CAST(end_date AS DATE)   IS NOT NULL
          AND TRY_CAST(start_date AS DATE) <= TRY_CAST(end_date AS DATE);

        SET @row_count = @@ROWCOUNT;
        SET @end_time  = GETDATE();
        PRINT '   ✅ ' + CAST(@row_count AS NVARCHAR) + ' rows loaded | '
              + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' ms';
    END TRY
    BEGIN CATCH
        PRINT '   ❌ ERROR: ' + ERROR_MESSAGE();
        THROW;
    END CATCH;

    -- ────────────────────────────────────────────────────────────
    -- TABLE 6 : silver.sales_transactions  (core fact — 15,000 rows)
    -- Transforms:
    --   • All ID columns     → INT
    --   • txn_date           → DATE
    --   • cases_sold         → INT
    --   • discount_pct       → DECIMAL(5,2)
    --   • revenue_usd        → DECIMAL(12,2)
    --   • is_valid flag      → 0 if revenue <= 0 or cases_sold <= 0
    --   • Dedup on txn_id
    -- ────────────────────────────────────────────────────────────
    BEGIN TRY
        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>> Transforming bronze.sales_transactions → silver.sales_transactions ...';

        TRUNCATE TABLE silver.sales_transactions;

        INSERT INTO silver.sales_transactions (
            txn_id, txn_date, account_id, rep_id, division,
            sku_id, supplier_id, cases_sold, discount_pct,
            revenue_usd, channel, is_valid
        )
        SELECT
            CAST(txn_id AS INT),
            TRY_CAST(txn_date AS DATE),
            CAST(account_id AS INT),
            CAST(rep_id AS INT),
            LTRIM(RTRIM(division)),
            CAST(sku_id AS INT),
            CAST(supplier_id AS INT),
            CAST(cases_sold AS INT),
            COALESCE(TRY_CAST(discount_pct AS DECIMAL(5,2)), 0),
            TRY_CAST(revenue_usd AS DECIMAL(12,2)),
            CASE
                WHEN LTRIM(RTRIM(UPPER(channel))) = 'ON-PREMISE'  THEN 'On-Premise'
                WHEN LTRIM(RTRIM(UPPER(channel))) = 'OFF-PREMISE' THEN 'Off-Premise'
                ELSE LTRIM(RTRIM(channel))
            END,
            -- Validation flag: revenue must be positive, cases must be > 0
            CASE
                WHEN TRY_CAST(revenue_usd AS DECIMAL(12,2)) <= 0 THEN 0
                WHEN TRY_CAST(cases_sold AS INT)             <= 0 THEN 0
                WHEN TRY_CAST(txn_date AS DATE)             IS NULL THEN 0
                ELSE 1
            END
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY txn_id
                    ORDER BY dwh_create_date DESC
                ) AS rn
            FROM bronze.sales_transactions
            WHERE txn_id IS NOT NULL
        ) dedup
        WHERE rn = 1;

        SET @row_count = @@ROWCOUNT;
        SET @end_time  = GETDATE();
        PRINT '   ✅ ' + CAST(@row_count AS NVARCHAR) + ' rows loaded | '
              + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' ms';
    END TRY
    BEGIN CATCH
        PRINT '   ❌ ERROR: ' + ERROR_MESSAGE();
        THROW;
    END CATCH;

    -- ────────────────────────────────────────────────────────────
    -- TABLE 7 : silver.rep_quota_actuals
    -- Transforms:
    --   • rep_id                 → INT
    --   • quota_cases, actual_cases → INT
    --   • attainment_pct         → DECIMAL(6,2)
    --   • revenue columns        → DECIMAL(14,2)
    --   • performance_flag       → derived from attainment_pct
    --                              ≥ 100  → 'Achieved'
    --                              75-99  → 'At Risk'
    --                              < 75   → 'Behind'
    -- ────────────────────────────────────────────────────────────
    BEGIN TRY
        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>> Transforming bronze.rep_quota_actuals → silver.rep_quota_actuals ...';

        TRUNCATE TABLE silver.rep_quota_actuals;

        INSERT INTO silver.rep_quota_actuals (
            rep_id, rep_name, division, month_year,
            quota_cases, actual_cases, attainment_pct,
            quota_revenue_usd, actual_revenue_usd, performance_flag
        )
        SELECT
            CAST(rep_id AS INT),
            LTRIM(RTRIM(rep_name)),
            LTRIM(RTRIM(division)),
            LTRIM(RTRIM(month)),              -- kept as 'YYYY-MM' string
            CAST(quota_cases AS INT),
            CAST(actual_cases AS INT),
            TRY_CAST(attainment_pct AS DECIMAL(6,2)),
            TRY_CAST(quota_revenue_usd  AS DECIMAL(14,2)),
            TRY_CAST(actual_revenue_usd AS DECIMAL(14,2)),
            -- Derive performance bucket from attainment percentage
            CASE
                WHEN TRY_CAST(attainment_pct AS DECIMAL(6,2)) >= 100 THEN 'Achieved'
                WHEN TRY_CAST(attainment_pct AS DECIMAL(6,2)) >= 75  THEN 'At Risk'
                ELSE 'Behind'
            END
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY rep_id, month
                    ORDER BY dwh_create_date DESC
                ) AS rn
            FROM bronze.rep_quota_actuals
            WHERE rep_id IS NOT NULL
              AND month  IS NOT NULL
        ) dedup
        WHERE rn = 1;

        SET @row_count = @@ROWCOUNT;
        SET @end_time  = GETDATE();
        PRINT '   ✅ ' + CAST(@row_count AS NVARCHAR) + ' rows loaded | '
              + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' ms';
    END TRY
    BEGIN CATCH
        PRINT '   ❌ ERROR: ' + ERROR_MESSAGE();
        THROW;
    END CATCH;

    -- ────────────────────────────────────────────────────────────
    -- TABLE 8 : silver.program_attainments
    -- Transforms:
    --   • All ID columns     → INT
    --   • goal_value, actual_value, attainment_pct, bonus → DECIMAL
    --   • status             → standardized ('Achieved' | 'At Risk' | 'Behind')
    --   • Dedup on attainment_id
    -- ────────────────────────────────────────────────────────────
    BEGIN TRY
        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>> Transforming bronze.program_attainments → silver.program_attainments ...';

        TRUNCATE TABLE silver.program_attainments;

        INSERT INTO silver.program_attainments (
            attainment_id, program_id, supplier_id, quarter,
            rep_id, rep_name, division, program_type,
            goal_value, actual_value, attainment_pct,
            status, bonus_earned_usd
        )
        SELECT
            CAST(attainment_id AS INT),
            CAST(program_id    AS INT),
            CAST(supplier_id   AS INT),
            LTRIM(RTRIM(quarter)),
            CAST(rep_id AS INT),
            LTRIM(RTRIM(rep_name)),
            LTRIM(RTRIM(division)),
            LTRIM(RTRIM(program_type)),
            TRY_CAST(goal_value       AS DECIMAL(12,2)),
            TRY_CAST(actual_value     AS DECIMAL(12,2)),
            TRY_CAST(attainment_pct   AS DECIMAL(6,2)),
            -- Standardize status values to consistent labels
            CASE
                WHEN LTRIM(RTRIM(UPPER(status))) = 'ACHIEVED' THEN 'Achieved'
                WHEN LTRIM(RTRIM(UPPER(status))) = 'AT RISK'  THEN 'At Risk'
                WHEN LTRIM(RTRIM(UPPER(status))) = 'BEHIND'   THEN 'Behind'
                ELSE LTRIM(RTRIM(status))
            END,
            TRY_CAST(bonus_earned_usd AS DECIMAL(12,2))
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY attainment_id
                    ORDER BY dwh_create_date DESC
                ) AS rn
            FROM bronze.program_attainments
            WHERE attainment_id IS NOT NULL
        ) dedup
        WHERE rn = 1;

        SET @row_count = @@ROWCOUNT;
        SET @end_time  = GETDATE();
        PRINT '   ✅ ' + CAST(@row_count AS NVARCHAR) + ' rows loaded | '
              + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' ms';
    END TRY
    BEGIN CATCH
        PRINT '   ❌ ERROR: ' + ERROR_MESSAGE();
        THROW;
    END CATCH;

    -- ────────────────────────────────────────────────────────────
    -- SUMMARY
    -- ────────────────────────────────────────────────────────────
    PRINT '';
    PRINT '============================================================';
    PRINT 'SILVER LOAD COMPLETE — all 8 tables transformed';
    PRINT 'End time : ' + CONVERT(NVARCHAR, GETDATE(), 120);
    PRINT '============================================================';
    PRINT '';
    PRINT 'Next step → EXEC gold.build_gold  (run after Silver is loaded)';

END;
GO

PRINT '✅  Stored procedure silver.load_silver created successfully.';
PRINT 'Run : EXEC silver.load_silver;';
