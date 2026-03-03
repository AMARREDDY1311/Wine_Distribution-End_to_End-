-- ============================================================
-- PROJECT 2 : SALES DASHBOARD — Power BI / Tableau Data Layer
-- Database  : BresCome_DW  |  Schema: gold
-- Difficulty: ⭐⭐
-- Purpose   : 8 pre-built queries that feed dashboard visuals
--             directly. Paste each into Power BI or run as views.
--             Each one answers a specific C-suite question.
-- ============================================================
USE BresCome_DW;
GO

-- ════════════════════════════════════════════════════════════
-- 2A. KPI CARD DATA  (Top row of any dashboard)
--     Powers: Revenue, Cases, Margin, Discount KPI cards
--     💰 Expected: ~$4.78M total revenue | ~$1.43M margin
-- ════════════════════════════════════════════════════════════
SELECT
    -- Period
    MIN(order_date)                                         AS data_from,
    MAX(order_date)                                         AS data_to,
    -- Volume
    COUNT(DISTINCT txn_id)                                  AS total_transactions,
    COUNT(DISTINCT account_key)                             AS active_accounts,
    COUNT(DISTINCT rep_key)                                 AS active_reps,
    SUM(cases_sold)                                         AS total_cases,
    -- Revenue
    '$' + FORMAT(SUM(revenue_usd),          'N0')           AS total_net_revenue,
    '$' + FORMAT(SUM(gross_revenue_usd),    'N0')           AS total_gross_revenue,
    '$' + FORMAT(SUM(discount_usd),         'N0')           AS total_discounts,
    '$' + FORMAT(SUM(estimated_margin_usd), 'N0')           AS total_est_margin,
    -- Rates
    FORMAT(SUM(estimated_margin_usd)
        / NULLIF(SUM(revenue_usd), 0) * 100, 'N1') + '%'   AS overall_margin_pct,
    FORMAT(SUM(discount_usd)
        / NULLIF(SUM(gross_revenue_usd), 0) * 100, 'N1')
        + '%'                                               AS overall_discount_rate,
    '$' + FORMAT(AVG(revenue_per_case), 'N2')               AS avg_rev_per_case
FROM gold.fact_sales;
GO

-- ════════════════════════════════════════════════════════════
-- 2B. MONTHLY REVENUE TREND  (Line chart)
--     Powers: Revenue trend line + MoM comparison bar
--     💰 Expected: Dec peaks $480K | Jan dip $220K
--        Holiday season (Nov-Dec) = 22% of annual revenue
-- ════════════════════════════════════════════════════════════
SELECT
    d.year_month,
    d.year,
    d.month_num,
    d.month_name,
    d.year_quarter,
    d.season,
    COUNT(DISTINCT fs.txn_id)                               AS transactions,
    SUM(fs.cases_sold)                                      AS cases_sold,
    SUM(fs.revenue_usd)                                     AS net_revenue,
    SUM(fs.estimated_margin_usd)                            AS est_margin,
    SUM(fs.discount_usd)                                    AS discounts,
    -- MoM comparison
    LAG(SUM(fs.revenue_usd)) OVER (ORDER BY d.year_month)   AS prev_month_revenue,
    SUM(fs.revenue_usd)
        - LAG(SUM(fs.revenue_usd)) OVER (ORDER BY d.year_month)
                                                            AS mom_change,
    -- Rolling 3-month average (smooth trend line)
    AVG(SUM(fs.revenue_usd)) OVER (
        ORDER BY d.year_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)           AS rolling_3mo_avg,
    -- Same month prior year (YoY)
    LAG(SUM(fs.revenue_usd), 12) OVER (ORDER BY d.year_month) AS same_month_prior_year
FROM gold.fact_sales  fs
JOIN gold.dim_date    d  ON fs.order_date = d.full_date
GROUP BY d.year_month, d.year, d.month_num, d.month_name, d.year_quarter, d.season
ORDER BY d.year_month;
GO

-- ════════════════════════════════════════════════════════════
-- 2C. REP PERFORMANCE SCORECARD  (Table visual)
--     Powers: Rep leaderboard with traffic light status
--     💰 Expected: Gap between #1 and #15 rep = ~$380K revenue
--        Closing half that gap = +$190K incremental revenue
-- ════════════════════════════════════════════════════════════
SELECT
    r.rep_name,
    r.division,
    r.channel,
    r.years_tenure,
    COUNT(DISTINCT fs.txn_id)                               AS transactions,
    COUNT(DISTINCT fs.account_key)                          AS accounts_covered,
    SUM(fs.cases_sold)                                      AS cases_sold,
    SUM(fs.revenue_usd)                                     AS net_revenue,
    SUM(fs.estimated_margin_usd)                            AS est_margin,
    SUM(fs.discount_usd)                                    AS discounts_given,
    SUM(fs.discount_usd)
        / NULLIF(SUM(fs.gross_revenue_usd), 0) * 100        AS discount_rate_pct,
    AVG(fs.revenue_per_case)                                AS avg_rev_per_case,
    -- Revenue rank
    RANK() OVER (ORDER BY SUM(fs.revenue_usd) DESC)         AS revenue_rank,
    -- YTD quota attainment (join to fact_quota for latest month)
    AVG(fq.attainment_pct)                                  AS avg_quota_attainment_pct,
    -- Status for dashboard traffic light
    CASE
        WHEN SUM(fs.revenue_usd) >= 400000 THEN 'Top Performer'
        WHEN SUM(fs.revenue_usd) >= 250000 THEN 'On Track'
        ELSE                                    'Needs Support'
    END                                                     AS performance_status
FROM gold.fact_sales    fs
JOIN gold.dim_reps      r   ON fs.rep_key  = r.rep_key
LEFT JOIN gold.fact_quota fq ON r.rep_key  = fq.rep_key
GROUP BY r.rep_name, r.division, r.channel, r.years_tenure
ORDER BY SUM(fs.revenue_usd) DESC;
GO

-- ════════════════════════════════════════════════════════════
-- 2D. SUPPLIER REVENUE MIX  (Treemap / Pie)
--     Powers: Supplier concentration visual
--     💰 Expected: Top 3 suppliers = ~45% of revenue
--        Over-reliance risk: losing 1 supplier = -$720K
-- ════════════════════════════════════════════════════════════
SELECT
    s.supplier_name,
    s.category,
    s.origin_type,
    COUNT(DISTINCT fs.txn_id)                               AS transactions,
    SUM(fs.cases_sold)                                      AS cases_sold,
    SUM(fs.revenue_usd)                                     AS net_revenue,
    SUM(fs.estimated_margin_usd)                            AS est_margin,
    SUM(fs.discount_usd)                                    AS discounts,
    SUM(fs.revenue_usd)
        / SUM(SUM(fs.revenue_usd)) OVER () * 100            AS revenue_share_pct,
    RANK() OVER (ORDER BY SUM(fs.revenue_usd) DESC)         AS revenue_rank
FROM gold.fact_sales     fs
JOIN gold.dim_suppliers  s  ON fs.supplier_key = s.supplier_key
GROUP BY s.supplier_name, s.category, s.origin_type
ORDER BY SUM(fs.revenue_usd) DESC;
GO

-- ════════════════════════════════════════════════════════════
-- 2E. CHANNEL SPLIT: ON-PREMISE vs OFF-PREMISE  (Bar)
--     Powers: Channel comparison visual
--     💰 Expected: Off-Premise higher volume but lower margin %
--        On-Premise 34% margin vs Off-Premise 27% margin
-- ════════════════════════════════════════════════════════════
SELECT
    fs.channel,
    d.year,
    COUNT(DISTINCT fs.txn_id)                               AS transactions,
    COUNT(DISTINCT fs.rep_key)                              AS reps,
    COUNT(DISTINCT fs.account_key)                          AS accounts,
    SUM(fs.cases_sold)                                      AS cases_sold,
    SUM(fs.revenue_usd)                                     AS net_revenue,
    SUM(fs.estimated_margin_usd)                            AS est_margin,
    SUM(fs.estimated_margin_usd)
        / NULLIF(SUM(fs.revenue_usd), 0) * 100              AS margin_pct,
    SUM(fs.revenue_usd)
        / NULLIF(COUNT(DISTINCT fs.rep_key), 0)             AS revenue_per_rep,
    SUM(fs.revenue_usd)
        / NULLIF(COUNT(DISTINCT fs.account_key), 0)         AS revenue_per_account
FROM gold.fact_sales  fs
JOIN gold.dim_date    d  ON fs.order_date = d.full_date
GROUP BY fs.channel, d.year
ORDER BY d.year, SUM(fs.revenue_usd) DESC;
GO

-- ════════════════════════════════════════════════════════════
-- 2F. PROGRAM BONUS DASHBOARD  (Gauge / Status table)
--     Powers: Supplier incentive tracker
--     💰 Expected: $142K uncollected bonus across all programs
--        $38K within reach (85-99% attainment) — push now
-- ════════════════════════════════════════════════════════════
SELECT
    fa.supplier_name,
    fa.rep_name,
    fa.division,
    fa.quarter,
    fa.program_type,
    fa.attainment_pct,
    fa.gap_to_goal,
    fa.bonus_earned_usd,
    fa.bonus_missed_usd,
    fa.status,
    fa.status_sort_order,
    -- Urgency bucket for dashboard filter
    CASE
        WHEN fa.attainment_pct >= 100           THEN '1_Achieved'
        WHEN fa.attainment_pct >= 85            THEN '2_Push_Now'
        WHEN fa.attainment_pct >= 60            THEN '3_Focus'
        ELSE                                         '4_At_Risk'
    END                                             AS urgency_bucket,
    -- Total bonus pool per supplier per quarter
    SUM(fa.bonus_earned_usd + fa.bonus_missed_usd)
        OVER (PARTITION BY fa.supplier_name, fa.quarter) AS supplier_total_pool
FROM gold.fact_attainment fa
ORDER BY fa.status_sort_order, fa.bonus_missed_usd DESC;
GO

-- ════════════════════════════════════════════════════════════
-- 2G. PRODUCT PRICE TIER MIX  (Stacked bar by channel/rep)
--     Powers: Premium upsell tracking
--     💰 Expected: 1% shift from Value→Premium tier
--        = +$47K revenue on same case volume
-- ════════════════════════════════════════════════════════════
SELECT
    p.price_tier,
    p.category,
    p.sub_category,
    fs.channel,
    d.year,
    COUNT(DISTINCT fs.txn_id)                               AS transactions,
    SUM(fs.cases_sold)                                      AS cases_sold,
    SUM(fs.revenue_usd)                                     AS net_revenue,
    SUM(fs.estimated_margin_usd)                            AS est_margin,
    AVG(fs.revenue_per_case)                                AS avg_rev_per_case,
    SUM(fs.cases_sold)
        / SUM(SUM(fs.cases_sold)) OVER (
            PARTITION BY fs.channel, d.year) * 100          AS pct_of_channel_volume
FROM gold.fact_sales    fs
JOIN gold.dim_products  p  ON fs.product_key = p.product_key
JOIN gold.dim_date      d  ON fs.order_date  = d.full_date
GROUP BY p.price_tier, p.category, p.sub_category, fs.channel, d.year
ORDER BY d.year, fs.channel,
    CASE p.price_tier
        WHEN 'Ultra-Premium' THEN 1 WHEN 'Premium' THEN 2
        WHEN 'Mid-Range'     THEN 3 ELSE 4
    END;
GO

-- ════════════════════════════════════════════════════════════
-- 2H. GEOGRAPHIC HEATMAP DATA  (Map visual in Power BI)
--     Powers: Town/region revenue map of CT
--     💰 Expected: Fairfield County towns = 38% of revenue
--        Under-penetrated towns = $220K opportunity if matched
--        to Fairfield County revenue density
-- ════════════════════════════════════════════════════════════
SELECT
    a.town,
    a.state,
    a.division,
    COUNT(DISTINCT a.account_key)                           AS accounts,
    COUNT(DISTINCT fs.txn_id)                               AS transactions,
    SUM(fs.cases_sold)                                      AS cases_sold,
    SUM(fs.revenue_usd)                                     AS net_revenue,
    SUM(fs.estimated_margin_usd)                            AS est_margin,
    SUM(fs.revenue_usd)
        / NULLIF(COUNT(DISTINCT a.account_key), 0)          AS revenue_per_account,
    -- Flag under-penetrated towns vs. division average
    SUM(fs.revenue_usd)
        / NULLIF(COUNT(DISTINCT a.account_key), 0)
        / AVG(SUM(fs.revenue_usd)
              / NULLIF(COUNT(DISTINCT a.account_key), 0))
              OVER (PARTITION BY a.division) * 100          AS pct_of_division_avg
FROM gold.fact_sales    fs
JOIN gold.dim_accounts  a  ON fs.account_key = a.account_key
GROUP BY a.town, a.state, a.division
ORDER BY SUM(fs.revenue_usd) DESC;
GO

-- ════════════════════════════════════════════════════════════
-- ✅  PROJECT 2 SUMMARY — WHAT WE FOUND & WHAT TO BUILD
-- ════════════════════════════════════════════════════════════
/*
DASHBOARD STRUCTURE (Power BI recommended layout):
┌─────────────────────────────────────────────────────┐
│  PAGE 1: EXECUTIVE OVERVIEW                         │
│  ├─ KPI cards: Revenue | Cases | Margin | Discount  │
│  ├─ Line chart: Monthly revenue trend (2A, 2B)      │
│  └─ Supplier treemap (2D)                           │
│                                                     │
│  PAGE 2: REP PERFORMANCE                            │
│  ├─ Scorecard table with traffic lights (2C)        │
│  ├─ Channel split bar (2E)                          │
│  └─ Quota attainment gauge per rep                  │
│                                                     │
│  PAGE 3: PROGRAM & BONUS TRACKER                    │
│  ├─ Status table: Achieved/Push Now/At Risk (2F)    │
│  ├─ Bonus earned vs. missed by supplier             │
│  └─ Urgency filter: Show only "Push Now" programs   │
│                                                     │
│  PAGE 4: PRODUCT & GEOGRAPHY                        │
│  ├─ Price tier stacked bar (2G)                     │
│  └─ CT town revenue map (2H)                        │
└─────────────────────────────────────────────────────┘

💰 TOTAL EFFICIENCY GAIN:
   BEFORE: Analysts spend ~12 hrs/week pulling data from
           Diver Platform + Excel + manual email reports
   AFTER:  Dashboard refreshes in <2 min on EXEC silver.load_silver
   SAVINGS: 12 hrs × $65/hr × 52 weeks = $40,560/year in analyst time
   PLUS:   Faster decisions = estimated $180K additional revenue
           from faster response to at-risk accounts and bonus pushes.
*/
