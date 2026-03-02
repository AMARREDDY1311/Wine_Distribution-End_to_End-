-- ============================================================
-- SCRIPT  : ddl_bronze.sql
-- PURPOSE : Define raw ingestion tables in the bronze schema.
--           Rules for this layer:
--             • Every column is NVARCHAR — no type enforcement yet
--             • Column names are an exact mirror of the CSV headers
--             • No constraints, no foreign keys, no indexes
--             • Two audit columns are added: dwh_create_date
--           Tables match the 8 source CSV files generated from
--           Brescome Barton's operational systems.
-- ============================================================

USE BresCome_DW;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

-- 1. sales_reps
CREATE OR ALTER VIEW bronze.v_dummy AS SELECT 1 AS x; -- prevents batch break issue
GO
DROP TABLE IF EXISTS bronze.sales_reps;
GO
CREATE TABLE bronze.sales_reps (
    rep_id          NVARCHAR(50),
    rep_name        NVARCHAR(100),
    division        NVARCHAR(100),
    channel         NVARCHAR(50),
    hire_date       NVARCHAR(20),
    email           NVARCHAR(150),
    dwh_create_date DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 2. suppliers
DROP TABLE IF EXISTS bronze.suppliers;
GO
CREATE TABLE bronze.suppliers (
    supplier_id     NVARCHAR(50),
    supplier_name   NVARCHAR(150),
    category        NVARCHAR(100),
    country         NVARCHAR(100),
    dwh_create_date DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 3. products
DROP TABLE IF EXISTS bronze.products;
GO
CREATE TABLE bronze.products (
    sku_id              NVARCHAR(50),
    product_name        NVARCHAR(200),
    supplier_id         NVARCHAR(50),
    category            NVARCHAR(100),
    sub_category        NVARCHAR(100),
    size_ml             NVARCHAR(20),
    price_per_case_usd  NVARCHAR(30),
    dwh_create_date     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 4. accounts
DROP TABLE IF EXISTS bronze.accounts;
GO
CREATE TABLE bronze.accounts (
    account_id      NVARCHAR(50),
    account_name    NVARCHAR(200),
    account_type    NVARCHAR(100),
    channel         NVARCHAR(50),
    town            NVARCHAR(100),
    state           NVARCHAR(10),
    division        NVARCHAR(100),
    rep_id          NVARCHAR(50),
    active          NVARCHAR(10),
    dwh_create_date DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 5. supplier_programs
DROP TABLE IF EXISTS bronze.supplier_programs;
GO
CREATE TABLE bronze.supplier_programs (
    program_id      NVARCHAR(50),
    supplier_id     NVARCHAR(50),
    quarter         NVARCHAR(20),
    start_date      NVARCHAR(20),
    end_date        NVARCHAR(20),
    program_type    NVARCHAR(100),
    goal_value      NVARCHAR(30),
    bonus_usd       NVARCHAR(30),
    division        NVARCHAR(100),
    dwh_create_date DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 6. sales_transactions
DROP TABLE IF EXISTS bronze.sales_transactions;
GO
CREATE TABLE bronze.sales_transactions (
    txn_id          NVARCHAR(50),
    txn_date        NVARCHAR(20),
    account_id      NVARCHAR(50),
    rep_id          NVARCHAR(50),
    division        NVARCHAR(100),
    sku_id          NVARCHAR(50),
    supplier_id     NVARCHAR(50),
    cases_sold      NVARCHAR(20),
    discount_pct    NVARCHAR(20),
    revenue_usd     NVARCHAR(30),
    channel         NVARCHAR(50),
    dwh_create_date DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 7. rep_quota_actuals
DROP TABLE IF EXISTS bronze.rep_quota_actuals;
GO
CREATE TABLE bronze.rep_quota_actuals (
    rep_id               NVARCHAR(50),
    rep_name             NVARCHAR(100),
    division             NVARCHAR(100),
    month                NVARCHAR(20),
    quota_cases          NVARCHAR(20),
    actual_cases         NVARCHAR(20),
    attainment_pct       NVARCHAR(20),
    quota_revenue_usd    NVARCHAR(30),
    actual_revenue_usd   NVARCHAR(30),
    dwh_create_date      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 8. program_attainments
DROP TABLE IF EXISTS bronze.program_attainments;
GO
CREATE TABLE bronze.program_attainments (
    attainment_id       NVARCHAR(50),
    program_id          NVARCHAR(50),
    supplier_id         NVARCHAR(50),
    quarter             NVARCHAR(20),
    rep_id              NVARCHAR(50),
    rep_name            NVARCHAR(100),
    division            NVARCHAR(100),
    program_type        NVARCHAR(100),
    goal_value          NVARCHAR(30),
    actual_value        NVARCHAR(30),
    attainment_pct      NVARCHAR(20),
    status              NVARCHAR(50),
    bonus_earned_usd    NVARCHAR(30),
    dwh_create_date     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

PRINT '✅ Bronze DDL created successfully';
