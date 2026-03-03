# BresCome Barton — Data Warehouse Documentation

> **Company:** Brescome Barton Inc. | CT wine/spirits distributor | ~$348M revenue  
> **Database:** `BresCome_DW` | **Server:** `SQLEXPRESS01` | **Engine:** SQL Server  

---

## Project Summary

Production-style data warehouse built using the **Medallion Architecture** (Bronze → Silver → Gold), with a **Kimball Star Schema** in the Gold layer. All ETL is T-SQL stored procedures.

| Layer | Schema | Objects | Load Method |
|-------|--------|---------|-------------|
| 🥉 Bronze | `bronze` | 8 tables | `EXEC bronze.load_bronze` (BULK INSERT) |
| 🥈 Silver | `silver` | 8 tables | `EXEC silver.load_silver` (INSERT SELECT) |
| 🥇 Gold | `gold` | 9 views | `ddl_gold.sql` (CREATE VIEW — always fresh) |

---

## Architecture: Medallion Layers

### 🥉 Bronze Layer
- **Purpose:** Raw ingestion — data lands exactly as it came from the source
- **All columns:** `NVARCHAR` (no type enforcement)
- **Load strategy:** Truncate + BULK INSERT
- **Audit column:** `dwh_create_date = GETDATE()`
- **Procedure:** `EXEC bronze.load_bronze`
- **Permission required:** `NT Service\MSSQL$SQLEXPRESS01` needs **Read** on the CSV folder

### 🥈 Silver Layer
- **Purpose:** Cleanse, type-cast, standardize, deduplicate
- **Transformations applied:**
  - `TRY_CAST()` for safe type conversion (INT, DATE, DECIMAL, BIT)
  - `LTRIM(RTRIM())` on all string columns
  - `ROW_NUMBER() OVER (PARTITION BY key ORDER BY dwh_create_date DESC)` deduplication
  - `COALESCE()` for NULL handling
  - Channel standardization → `'On-Premise'` | `'Off-Premise'`
  - Performance buckets → `'Achieved'` | `'At Risk'` | `'Behind'`
  - Derived columns: `is_valid`, `performance_flag`
- **Procedure:** `EXEC silver.load_silver`

### 🥇 Gold Layer
- **Purpose:** Analytics-ready Star Schema for BI tools
- **Object type:** Views only (no physical tables — always reads live from Silver)
- **Surrogate keys:** `ROW_NUMBER() OVER (ORDER BY source_id)` on every dimension
- **Business logic:** price_tier, origin_type, ytd_actual_cases, gap_to_goal, bonus_missed_usd, season flags
- **Script:** `ddl_gold.sql`

---

## Source-to-Target Mapping

| Source CSV | Bronze | Silver | Gold | Rows |
|---|---|---|---|---|
| `01_sales_reps.csv` | `bronze.sales_reps` | `silver.sales_reps` | `dim_reps`, `fact_quota` | 15 |
| `02_suppliers.csv` | `bronze.suppliers` | `silver.suppliers` | `dim_suppliers` | 15 |
| `03_products.csv` | `bronze.products` | `silver.products` | `dim_products` | 25 |
| `04_accounts.csv` | `bronze.accounts` | `silver.accounts` | `dim_accounts` | 185 |
| `05_supplier_programs.csv` | `bronze.supplier_programs` | `silver.supplier_programs` | `dim_programs`, `fact_attainment` | 173 |
| `06_sales_transactions.csv` | `bronze.sales_transactions` | `silver.sales_transactions` | `fact_sales` | 15,000 |
| `07_rep_quota_actuals.csv` | `bronze.rep_quota_actuals` | `silver.rep_quota_actuals` | `fact_quota` | 360 |
| `08_program_attainments.csv` | `bronze.program_attainments` | `silver.program_attainments` | `fact_attainment` | 519 |
| *(computed)* | — | — | `dim_date` | 730 |

---

## Star Schema (Gold Layer)

```
         [dim_reps]      [dim_date]     [dim_accounts]
              \               |               /
               \              |              /
        [dim_products] -- [fact_sales] -- [dim_suppliers]
                                |
                         [dim_programs]

         [dim_reps] -------- [fact_quota]

  [dim_reps] + [dim_suppliers] + [dim_programs] -- [fact_attainment]
```

### Dimensions

| View | Grain | Surrogate Key | Key Derived Columns |
|------|-------|--------------|---------------------|
| `gold.dim_reps` | 1 row per rep | `rep_key` | `years_tenure` |
| `gold.dim_accounts` | 1 row per account | `account_key` | `account_status` |
| `gold.dim_products` | 1 row per SKU | `product_key` | `price_tier` (Value/Mid-Range/Premium/Ultra-Premium) |
| `gold.dim_suppliers` | 1 row per supplier | `supplier_key` | `origin_type` (Domestic/Import) |
| `gold.dim_programs` | 1 row per program | `program_key` | `program_duration_days` |
| `gold.dim_date` | 1 row per day | `full_date` (NK) | `season`, `is_weekend`, `year_quarter` |

### Facts

| View | Grain | Key Measures | Rows |
|------|-------|-------------|------|
| `gold.fact_sales` | 1 per transaction | `cases_sold`, `revenue_usd`, `discount_usd`, `gross_revenue_usd`, `revenue_per_case`, `estimated_margin_usd` | ~15,000 |
| `gold.fact_quota` | 1 per rep per month | `quota_cases`, `actual_cases`, `attainment_pct`, `ytd_actual_cases`, `revenue_variance_usd`, `performance_flag` | 360 |
| `gold.fact_attainment` | 1 per rep per program | `goal_value`, `actual_value`, `attainment_pct`, `bonus_earned_usd`, `gap_to_goal`, `bonus_missed_usd`, `status_sort_order` | 519 |

---

## Naming Conventions

### Schemas
| Schema | Layer | Object Type | Purpose |
|--------|-------|-------------|---------|
| `bronze` | Bronze | Tables | Raw data as-is from source |
| `silver` | Silver | Tables | Cleansed, typed, validated |
| `gold` | Gold | Views | Analytics-ready star schema |

### Column Patterns
| Pattern | Example | Where | Rule |
|---------|---------|-------|------|
| `{entity}_id` | `rep_id` | Bronze + Silver | Source natural key |
| `{entity}_key` | `rep_key` | Gold only | Surrogate key (ROW_NUMBER) |
| `{entity}_src_id` | `rep_src_id` | Gold only | Source ID retained for lineage |
| `is_{flag}` | `is_active`, `is_valid` | Silver + Gold | BIT boolean columns |
| `dwh_create_date` | `dwh_create_date` | All layers | Audit — GETDATE() on load |
| `dwh_source` | `dwh_source` | Silver only | Source table name for lineage |
| `{measure}_usd` | `revenue_usd` | All layers | Currency — unit in suffix |
| `{measure}_pct` | `attainment_pct` | All layers | Percentage columns |

### Procedures
| Procedure | Pattern | Purpose |
|-----------|---------|---------|
| `bronze.load_bronze` | `{schema}.load_{schema}` | Load all bronze tables from CSV |
| `silver.load_silver` | `{schema}.load_{schema}` | ETL bronze → silver |

---

## Data Quality Checks

Run `quality_checks_silver.sql` after every `EXEC silver.load_silver`.

| Check Category | What Is Validated | Expected Result |
|---|---|---|
| NULL / Missing Keys | `rep_id`, `txn_date`, `revenue_usd`, `price_per_case_usd` | 0 rows |
| Duplicate Keys | `rep_id`, `txn_id`, `sku_id`, rep+month quota | 0 rows |
| Date Validation | Transactions in range, programs start < end, hire dates | 0 rows |
| Standardization | Channel values, category values, performance_flag | 0 rows |
| Referential Integrity | Orphan transactions, orphan accounts | 0 rows |
| Row Counts | All 8 tables match expected counts | See table above |

---

## Run Order

```sql
-- ── ONE-TIME SETUP ──────────────────────────────────────────
-- 1. Run init_database.sql          → creates BresCome_DW + schemas
-- 2. Run bronze/ddl_bronze.sql      → 8 bronze tables
-- 3. Run bronze/proc_load_bronze.sql → creates bronze.load_bronze procedure
-- 4. Run silver/ddl_silver.sql      → 8 silver tables
-- 5. Run silver/proc_load_silver.sql → creates silver.load_silver procedure
-- 6. Run gold/ddl_gold.sql          → 9 gold views

-- ── EVERY REFRESH ───────────────────────────────────────────
EXEC bronze.load_bronze;    -- Step 1: CSV → bronze
EXEC silver.load_silver;    -- Step 2: bronze → silver (ETL)
-- Step 3: run quality_checks_silver.sql manually
-- Gold is always fresh — no load needed

-- ── QUERY ANALYTICS ─────────────────────────────────────────
SELECT TOP 100 * FROM gold.fact_sales;
SELECT * FROM gold.fact_quota   ORDER BY attainment_pct DESC;
SELECT * FROM gold.fact_attainment WHERE status = 'Behind';
```

---

## File Inventory

| File | Purpose | Run When |
|------|---------|----------|
| `init_database.sql` | Create DB + 3 schemas | Once |
| `bronze/ddl_bronze.sql` | Create 8 bronze tables | Once |
| `bronze/proc_load_bronze.sql` | Create load_bronze procedure | Once |
| `silver/ddl_silver.sql` | Create 8 silver tables | Once |
| `silver/proc_load_silver.sql` | Create load_silver procedure | Once |
| `silver/quality_checks_silver.sql` | 15 validation queries | After every silver load |
| `gold/ddl_gold.sql` | Create 9 gold views | Once (re-run if schema changes) |

---

*BresCome Barton Data Warehouse · SQL Server SQLEXPRESS01 · Jan 2024 – Dec 2025*
