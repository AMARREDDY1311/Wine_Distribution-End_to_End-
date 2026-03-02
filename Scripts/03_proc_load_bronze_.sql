-- ============================================================
-- SCRIPT  : proc_load_bronze_v3.sql
-- ============================================================
USE BresCome_DW;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @error_count INT = 0;

    PRINT '============================================================';
    PRINT 'BRONZE LOAD STARTED';
    PRINT '============================================================';

    ------------------------------------------------------------
    -- 1. SALES_REPS
    ------------------------------------------------------------
    BEGIN TRY
        DROP TABLE IF EXISTS #stg_sales_reps;
        CREATE TABLE #stg_sales_reps (
            rep_id NVARCHAR(50),
            rep_name NVARCHAR(100),
            division NVARCHAR(100),
            channel NVARCHAR(50),
            hire_date NVARCHAR(20),
            email NVARCHAR(150)
        );

        BULK INSERT #stg_sales_reps
        FROM 'C:\temp\Wine\01_sales_reps.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

        TRUNCATE TABLE bronze.sales_reps;

        INSERT INTO bronze.sales_reps
        (rep_id, rep_name, division, channel, hire_date, email)
        SELECT rep_id, rep_name, division, channel, hire_date, email
        FROM #stg_sales_reps;

        PRINT 'sales_reps loaded';
    END TRY
    BEGIN CATCH
        SET @error_count += 1;
        PRINT ERROR_MESSAGE();
    END CATCH;

    ------------------------------------------------------------
    -- 2. SUPPLIERS
    ------------------------------------------------------------
    BEGIN TRY
        DROP TABLE IF EXISTS #stg_suppliers;
        CREATE TABLE #stg_suppliers (
            supplier_id NVARCHAR(50),
            supplier_name NVARCHAR(150),
            category NVARCHAR(100),
            country NVARCHAR(100)
        );

        BULK INSERT #stg_suppliers
        FROM 'C:\temp\Wine\02_suppliers.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

        TRUNCATE TABLE bronze.suppliers;

        INSERT INTO bronze.suppliers
        (supplier_id, supplier_name, category, country)
        SELECT supplier_id, supplier_name, category, country
        FROM #stg_suppliers;

        PRINT 'suppliers loaded';
    END TRY
    BEGIN CATCH
        SET @error_count += 1;
        PRINT ERROR_MESSAGE();
    END CATCH;

    ------------------------------------------------------------
    -- 3. PRODUCTS
    ------------------------------------------------------------
    BEGIN TRY
        DROP TABLE IF EXISTS #stg_products;
        CREATE TABLE #stg_products (
            sku_id NVARCHAR(50),
            product_name NVARCHAR(200),
            supplier_id NVARCHAR(50),
            category NVARCHAR(100),
            sub_category NVARCHAR(100),
            size_ml NVARCHAR(20),
            price_per_case_usd NVARCHAR(30)
        );

        BULK INSERT #stg_products
        FROM 'C:\temp\Wine\03_products.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

        TRUNCATE TABLE bronze.products;

        INSERT INTO bronze.products
        (sku_id, product_name, supplier_id, category, sub_category, size_ml, price_per_case_usd)
        SELECT sku_id, product_name, supplier_id, category, sub_category, size_ml, price_per_case_usd
        FROM #stg_products;

        PRINT 'products loaded';
    END TRY
    BEGIN CATCH
        SET @error_count += 1;
        PRINT ERROR_MESSAGE();
    END CATCH;

    ------------------------------------------------------------
    -- 4. ACCOUNTS
    ------------------------------------------------------------
    BEGIN TRY
        DROP TABLE IF EXISTS #stg_accounts;
        CREATE TABLE #stg_accounts (
            account_id NVARCHAR(50),
            account_name NVARCHAR(200),
            account_type NVARCHAR(100),
            channel NVARCHAR(50),
            town NVARCHAR(100),
            state NVARCHAR(10),
            division NVARCHAR(100),
            rep_id NVARCHAR(50),
            active NVARCHAR(10)
        );

        BULK INSERT #stg_accounts
        FROM 'C:\temp\Wine\04_accounts.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

        TRUNCATE TABLE bronze.accounts;

        INSERT INTO bronze.accounts
        (account_id, account_name, account_type, channel, town, state, division, rep_id, active)
        SELECT account_id, account_name, account_type, channel, town, state, division, rep_id, active
        FROM #stg_accounts;

        PRINT 'accounts loaded';
    END TRY
    BEGIN CATCH
        SET @error_count += 1;
        PRINT ERROR_MESSAGE();
    END CATCH;

    ------------------------------------------------------------
    -- 5. SUPPLIER_PROGRAMS
    ------------------------------------------------------------
    BEGIN TRY
        DROP TABLE IF EXISTS #stg_supplier_programs;
        CREATE TABLE #stg_supplier_programs (
            program_id NVARCHAR(50),
            supplier_id NVARCHAR(50),
            quarter NVARCHAR(20),
            start_date NVARCHAR(20),
            end_date NVARCHAR(20),
            program_type NVARCHAR(100),
            goal_value NVARCHAR(30),
            bonus_usd NVARCHAR(30),
            division NVARCHAR(100)
        );

        BULK INSERT #stg_supplier_programs
        FROM 'C:\temp\Wine\05_supplier_programs.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

        TRUNCATE TABLE bronze.supplier_programs;

        INSERT INTO bronze.supplier_programs
        (program_id, supplier_id, quarter, start_date, end_date, program_type, goal_value, bonus_usd, division)
        SELECT program_id, supplier_id, quarter, start_date, end_date, program_type, goal_value, bonus_usd, division
        FROM #stg_supplier_programs;

        PRINT 'supplier_programs loaded';
    END TRY
    BEGIN CATCH
        SET @error_count += 1;
        PRINT ERROR_MESSAGE();
    END CATCH;

    ------------------------------------------------------------
    -- 6. SALES_TRANSACTIONS
    ------------------------------------------------------------
    BEGIN TRY
        DROP TABLE IF EXISTS #stg_sales_transactions;
        CREATE TABLE #stg_sales_transactions (
            txn_id NVARCHAR(50),
            txn_date NVARCHAR(20),
            account_id NVARCHAR(50),
            rep_id NVARCHAR(50),
            division NVARCHAR(100),
            sku_id NVARCHAR(50),
            supplier_id NVARCHAR(50),
            cases_sold NVARCHAR(20),
            discount_pct NVARCHAR(20),
            revenue_usd NVARCHAR(30),
            channel NVARCHAR(50)
        );

        BULK INSERT #stg_sales_transactions
        FROM 'C:\temp\Wine\06_sales_transactions.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

        TRUNCATE TABLE bronze.sales_transactions;

        INSERT INTO bronze.sales_transactions
        (txn_id, txn_date, account_id, rep_id, division, sku_id, supplier_id, cases_sold, discount_pct, revenue_usd, channel)
        SELECT txn_id, txn_date, account_id, rep_id, division, sku_id, supplier_id, cases_sold, discount_pct, revenue_usd, channel
        FROM #stg_sales_transactions;

        PRINT 'sales_transactions loaded';
    END TRY
    BEGIN CATCH
        SET @error_count += 1;
        PRINT ERROR_MESSAGE();
    END CATCH;

    ------------------------------------------------------------
    -- 7. REP_QUOTA_ACTUALS
    ------------------------------------------------------------
    BEGIN TRY
        DROP TABLE IF EXISTS #stg_rep_quota_actuals;
        CREATE TABLE #stg_rep_quota_actuals (
            rep_id NVARCHAR(50),
            rep_name NVARCHAR(100),
            division NVARCHAR(100),
            month NVARCHAR(20),
            quota_cases NVARCHAR(20),
            actual_cases NVARCHAR(20),
            attainment_pct NVARCHAR(20),
            quota_revenue_usd NVARCHAR(30),
            actual_revenue_usd NVARCHAR(30)
        );

        BULK INSERT #stg_rep_quota_actuals
        FROM 'C:\temp\Wine\07_rep_quota_actuals.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

        TRUNCATE TABLE bronze.rep_quota_actuals;

        INSERT INTO bronze.rep_quota_actuals
        (rep_id, rep_name, division, month, quota_cases, actual_cases, attainment_pct, quota_revenue_usd, actual_revenue_usd)
        SELECT rep_id, rep_name, division, month, quota_cases, actual_cases, attainment_pct, quota_revenue_usd, actual_revenue_usd
        FROM #stg_rep_quota_actuals;

        PRINT 'rep_quota_actuals loaded';
    END TRY
    BEGIN CATCH
        SET @error_count += 1;
        PRINT ERROR_MESSAGE();
    END CATCH;

    ------------------------------------------------------------
    -- 8. PROGRAM_ATTAINMENTS
    ------------------------------------------------------------
    BEGIN TRY
        DROP TABLE IF EXISTS #stg_program_attainments;
        CREATE TABLE #stg_program_attainments (
            attainment_id NVARCHAR(50),
            program_id NVARCHAR(50),
            supplier_id NVARCHAR(50),
            quarter NVARCHAR(20),
            rep_id NVARCHAR(50),
            rep_name NVARCHAR(100),
            division NVARCHAR(100),
            program_type NVARCHAR(100),
            goal_value NVARCHAR(30),
            actual_value NVARCHAR(30),
            attainment_pct NVARCHAR(20),
            status NVARCHAR(50),
            bonus_earned_usd NVARCHAR(30)
        );

        BULK INSERT #stg_program_attainments
        FROM 'C:\temp\Wine\08_program_attainments.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

        TRUNCATE TABLE bronze.program_attainments;

        INSERT INTO bronze.program_attainments
        (attainment_id, program_id, supplier_id, quarter, rep_id, rep_name, division, program_type, goal_value, actual_value, attainment_pct, status, bonus_earned_usd)
        SELECT attainment_id, program_id, supplier_id, quarter, rep_id, rep_name, division, program_type, goal_value, actual_value, attainment_pct, status, bonus_earned_usd
        FROM #stg_program_attainments;

        PRINT 'program_attainments loaded';
    END TRY
    BEGIN CATCH
        SET @error_count += 1;
        PRINT ERROR_MESSAGE();
    END CATCH;

    PRINT '============================================================';
    IF @error_count = 0
        PRINT '✅ BRONZE LOAD COMPLETED SUCCESSFULLY';
    ELSE
        PRINT '⚠️ Some tables failed to load';
    PRINT '============================================================';

END;
GO
