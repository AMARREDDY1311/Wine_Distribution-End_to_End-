-- ============================================================
-- SCRIPT  : init_database.sql
-- PURPOSE : Create the DataWarehouse database and the three
--           Medallion Architecture schemas:
--             bronze  → raw ingestion (as-is from source)
--             silver  → cleansed and standardized
--             gold    → analytics-ready star schema
-- PROJECT : Brescome Barton — Beverage Distribution DW
-- ============================================================

USE master;
GO

-- ── Drop and recreate the database ──────────────────────────
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'BresCome_DW')
BEGIN
    ALTER DATABASE BresCome_DW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE BresCome_DW;
END
GO

CREATE DATABASE BresCome_DW;
GO

USE BresCome_DW;
GO

-- ── Create schemas ───────────────────────────────────────────
-- BRONZE : Raw layer — data lands here exactly as it left the source
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

-- SILVER : Cleansed layer — standardized types, deduplicated, validated
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

-- GOLD   : Analytics layer — star schema views ready for Power BI / Tableau
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

PRINT '✅  BresCome_DW database created with bronze / silver / gold schemas.';
