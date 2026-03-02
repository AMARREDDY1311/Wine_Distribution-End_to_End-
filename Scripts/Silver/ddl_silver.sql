-- ============================================================
-- SCRIPT  : ddl_silver.sql
-- PURPOSE : Define the Silver layer tables.
--           Rules for this layer:
--             • Proper data types enforced (INT, DATE, DECIMAL, BIT)
--             • Columns are trimmed and standardized
--             • Duplicates removed via ROW_NUMBER() windowing
--             • Nulls handled with COALESCE / CASE
--             • Two audit columns added: dwh_create_date, dwh_source
--             • Source IDs preserved as-is (no surrogate keys yet —
--               those are generated in the Gold layer)
-- ============================================================

USE BresCome_DW;
GO

-- ────────────────────────────────────────────────────────────
-- 1.  silver.sales_reps
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('silver.sales_reps', 'U') IS NOT NULL
    DROP TABLE silver.sales_reps;
GO

CREATE TABLE silver.sales_reps (
    rep_id          INT             NOT NULL,
    rep_name        NVARCHAR(100)   NOT NULL,
    division        NVARCHAR(100)   NOT NULL,
    channel         NVARCHAR(50)    NOT NULL,   -- 'On-Premise' | 'Off-Premise'
    hire_date       DATE            NULL,
    email           NVARCHAR(150)   NULL,
    dwh_create_date DATETIME2       DEFAULT GETDATE(),
    dwh_source      NVARCHAR(50)    DEFAULT 'bronze.sales_reps'
);
GO

-- ────────────────────────────────────────────────────────────
-- 2.  silver.suppliers
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('silver.suppliers', 'U') IS NOT NULL
    DROP TABLE silver.suppliers;
GO

CREATE TABLE silver.suppliers (
    supplier_id     INT             NOT NULL,
    supplier_name   NVARCHAR(150)   NOT NULL,
    category        NVARCHAR(100)   NOT NULL,   -- 'Wine' | 'Spirits' | 'Wine & Spirits'
    country         NVARCHAR(100)   NULL,
    dwh_create_date DATETIME2       DEFAULT GETDATE(),
    dwh_source      NVARCHAR(50)    DEFAULT 'bronze.suppliers'
);
GO

-- ────────────────────────────────────────────────────────────
-- 3.  silver.products
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('silver.products', 'U') IS NOT NULL
    DROP TABLE silver.products;
GO

CREATE TABLE silver.products (
    sku_id              INT             NOT NULL,
    product_name        NVARCHAR(200)   NOT NULL,
    supplier_id         INT             NOT NULL,
    category            NVARCHAR(100)   NOT NULL,   -- 'Wine' | 'Spirits'
    sub_category        NVARCHAR(100)   NULL,        -- 'Red Wine' | 'Vodka' | 'Bourbon' etc.
    size_ml             INT             NULL,
    price_per_case_usd  DECIMAL(10,2)   NULL,
    dwh_create_date     DATETIME2       DEFAULT GETDATE(),
    dwh_source          NVARCHAR(50)    DEFAULT 'bronze.products'
);
GO

-- ────────────────────────────────────────────────────────────
-- 4.  silver.accounts
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('silver.accounts', 'U') IS NOT NULL
    DROP TABLE silver.accounts;
GO

CREATE TABLE silver.accounts (
    account_id      INT             NOT NULL,
    account_name    NVARCHAR(200)   NOT NULL,
    account_type    NVARCHAR(100)   NOT NULL,
    channel         NVARCHAR(50)    NOT NULL,   -- 'On-Premise' | 'Off-Premise'
    town            NVARCHAR(100)   NULL,
    state           NVARCHAR(10)    NULL,
    division        NVARCHAR(100)   NOT NULL,
    rep_id          INT             NOT NULL,
    is_active       BIT             NOT NULL,   -- renamed from 'active' → cleaner
    dwh_create_date DATETIME2       DEFAULT GETDATE(),
    dwh_source      NVARCHAR(50)    DEFAULT 'bronze.accounts'
);
GO

-- ────────────────────────────────────────────────────────────
-- 5.  silver.supplier_programs
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('silver.supplier_programs', 'U') IS NOT NULL
    DROP TABLE silver.supplier_programs;
GO

CREATE TABLE silver.supplier_programs (
    program_id      INT             NOT NULL,
    supplier_id     INT             NOT NULL,
    quarter         NVARCHAR(20)    NOT NULL,   -- '2024-Q1' format
    start_date      DATE            NOT NULL,
    end_date        DATE            NOT NULL,
    program_type    NVARCHAR(100)   NOT NULL,   -- 'Volume (cases)' | 'Account Penetration' etc.
    goal_value      DECIMAL(12,2)   NOT NULL,
    bonus_usd       DECIMAL(12,2)   NOT NULL,
    division        NVARCHAR(100)   NOT NULL,
    dwh_create_date DATETIME2       DEFAULT GETDATE(),
    dwh_source      NVARCHAR(50)    DEFAULT 'bronze.supplier_programs'
);
GO

-- ────────────────────────────────────────────────────────────
-- 6.  silver.sales_transactions
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('silver.sales_transactions', 'U') IS NOT NULL
    DROP TABLE silver.sales_transactions;
GO

CREATE TABLE silver.sales_transactions (
    txn_id          INT             NOT NULL,
    txn_date        DATE            NOT NULL,
    account_id      INT             NOT NULL,
    rep_id          INT             NOT NULL,
    division        NVARCHAR(100)   NOT NULL,
    sku_id          INT             NOT NULL,
    supplier_id     INT             NOT NULL,
    cases_sold      INT             NOT NULL,
    discount_pct    DECIMAL(5,2)    NOT NULL DEFAULT 0,
    revenue_usd     DECIMAL(12,2)   NOT NULL,
    channel         NVARCHAR(50)    NOT NULL,
    -- Derived flag: mark any transaction with invalid logic for review
    is_valid        BIT             NOT NULL DEFAULT 1,
    dwh_create_date DATETIME2       DEFAULT GETDATE(),
    dwh_source      NVARCHAR(50)    DEFAULT 'bronze.sales_transactions'
);
GO

-- ────────────────────────────────────────────────────────────
-- 7.  silver.rep_quota_actuals
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('silver.rep_quota_actuals', 'U') IS NOT NULL
    DROP TABLE silver.rep_quota_actuals;
GO

CREATE TABLE silver.rep_quota_actuals (
    rep_id               INT             NOT NULL,
    rep_name             NVARCHAR(100)   NOT NULL,
    division             NVARCHAR(100)   NOT NULL,
    month_year           NVARCHAR(20)    NOT NULL,   -- 'YYYY-MM'
    quota_cases          INT             NOT NULL,
    actual_cases         INT             NOT NULL,
    attainment_pct       DECIMAL(6,2)    NOT NULL,
    quota_revenue_usd    DECIMAL(14,2)   NOT NULL,
    actual_revenue_usd   DECIMAL(14,2)   NOT NULL,
    -- Derived: performance bucket for dashboarding
    performance_flag     NVARCHAR(20)    NOT NULL,   -- 'Achieved' | 'At Risk' | 'Behind'
    dwh_create_date      DATETIME2       DEFAULT GETDATE(),
    dwh_source           NVARCHAR(50)    DEFAULT 'bronze.rep_quota_actuals'
);
GO

-- ────────────────────────────────────────────────────────────
-- 8.  silver.program_attainments
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('silver.program_attainments', 'U') IS NOT NULL
    DROP TABLE silver.program_attainments;
GO

CREATE TABLE silver.program_attainments (
    attainment_id       INT             NOT NULL,
    program_id          INT             NOT NULL,
    supplier_id         INT             NOT NULL,
    quarter             NVARCHAR(20)    NOT NULL,
    rep_id              INT             NOT NULL,
    rep_name            NVARCHAR(100)   NOT NULL,
    division            NVARCHAR(100)   NOT NULL,
    program_type        NVARCHAR(100)   NOT NULL,
    goal_value          DECIMAL(12,2)   NOT NULL,
    actual_value        DECIMAL(12,2)   NOT NULL,
    attainment_pct      DECIMAL(6,2)    NOT NULL,
    status              NVARCHAR(20)    NOT NULL,   -- 'Achieved' | 'At Risk' | 'Behind'
    bonus_earned_usd    DECIMAL(12,2)   NOT NULL,
    dwh_create_date     DATETIME2       DEFAULT GETDATE(),
    dwh_source          NVARCHAR(50)    DEFAULT 'bronze.program_attainments'
);
GO

PRINT '✅  All 8 silver tables created successfully.';
