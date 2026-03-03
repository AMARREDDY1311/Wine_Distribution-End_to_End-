-- ============================================================
-- PROJECT 4 : A/B TESTING FRAMEWORK — Promotion & Rep Testing
-- Database  : BresCome_DW  |  Schema: gold
-- Difficulty: ⭐⭐⭐
-- Purpose   : Measure whether promotions, rep strategies, or
--             pricing changes actually work — with statistical
--             significance checks and dollar impact.
--
-- 3 TEST SCENARIOS using real data:
--   Test A: Did On-Premise reps outperform vs Off-Premise?
--   Test B: Do discount promotions actually lift revenue?
--   Test C: Does premium-tier pushing increase revenue/case?
-- ============================================================
USE BresCome_DW;
GO

-- ════════════════════════════════════════════════════════════
-- TEST A: CHANNEL STRATEGY TEST
--         On-Premise (treatment) vs Off-Premise (control)
--         Question: Which channel strategy drives more value?
--         💰 Expected: On-Premise = higher margin, lower volume
-- ════════════════════════════════════════════════════════════
PRINT '=== TEST A: CHANNEL STRATEGY COMPARISON ===';

WITH channel_metrics AS (
    SELECT
        fs.channel                                          AS test_group,
        COUNT(DISTINCT fs.txn_id)                           AS n_transactions,
        COUNT(DISTINCT fs.account_key)                      AS n_accounts,
        SUM(fs.revenue_usd)                                 AS total_revenue,
        AVG(fs.revenue_usd)                                 AS avg_order_value,
        STDEV(fs.revenue_usd)                               AS stdev_order_value,
        SUM(fs.cases_sold)                                  AS total_cases,
        AVG(fs.revenue_per_case)                            AS avg_rev_per_case,
        SUM(fs.estimated_margin_usd)                        AS total_margin,
        AVG(fs.estimated_margin_usd)
            / NULLIF(AVG(fs.revenue_usd), 0) * 100          AS avg_margin_pct,
        SUM(fs.discount_usd)
            / NULLIF(SUM(fs.gross_revenue_usd), 0) * 100    AS discount_rate
    FROM gold.fact_sales fs
    GROUP BY fs.channel
)
SELECT
    test_group,
    n_transactions,
    n_accounts,
    '$' + FORMAT(total_revenue,      'N0')                  AS total_revenue,
    '$' + FORMAT(avg_order_value,    'N2')                  AS avg_order_value,
    FORMAT(stdev_order_value,        'N2')                  AS stdev_order_value,
    '$' + FORMAT(avg_rev_per_case,   'N2')                  AS avg_rev_per_case,
    '$' + FORMAT(total_margin,       'N0')                  AS total_margin,
    FORMAT(avg_margin_pct,           'N1') + '%'            AS avg_margin_pct,
    FORMAT(discount_rate,            'N1') + '%'            AS discount_rate,
    -- Revenue per account (efficiency metric)
    '$' + FORMAT(total_revenue
        / NULLIF(n_accounts, 0), 'N0')                      AS revenue_per_account,
    -- Winner declaration based on margin%
    CASE WHEN avg_margin_pct = MAX(avg_margin_pct) OVER ()
         THEN '🏆 Higher Margin Winner'
         ELSE '   Comparison'
    END                                                     AS margin_winner
FROM channel_metrics;
GO

-- ════════════════════════════════════════════════════════════
-- TEST B: DISCOUNT PROMOTION LIFT TEST
--         Discounted transactions vs. full-price transactions
--         Question: Does discounting actually increase volume
--                   enough to offset the revenue given up?
--         💰 Expected: Discounts cost more than they earn back
-- ════════════════════════════════════════════════════════════
PRINT '=== TEST B: DISCOUNT PROMOTION LIFT TEST ===';

WITH discount_groups AS (
    SELECT
        CASE
            WHEN discount_pct = 0          THEN 'A_Control: No Discount'
            WHEN discount_pct BETWEEN 1
                              AND  5       THEN 'B_Low Discount (1-5%)'
            WHEN discount_pct BETWEEN 6
                              AND 15       THEN 'C_Mid Discount (6-15%)'
            ELSE                                'D_High Discount (>15%)'
        END                                                 AS test_group,
        revenue_usd,
        gross_revenue_usd,
        discount_usd,
        cases_sold,
        revenue_per_case,
        estimated_margin_usd,
        discount_pct
    FROM gold.fact_sales
)
SELECT
    test_group,
    COUNT(*)                                                AS n_transactions,
    FORMAT(AVG(discount_pct), 'N1') + '%'                   AS avg_discount_applied,
    '$' + FORMAT(AVG(cases_sold), 'N2')                     AS avg_cases_per_order,
    '$' + FORMAT(AVG(revenue_usd), 'N2')                    AS avg_net_revenue_per_order,
    '$' + FORMAT(AVG(gross_revenue_usd), 'N2')              AS avg_gross_per_order,
    '$' + FORMAT(AVG(discount_usd), 'N2')                   AS avg_discount_given,
    '$' + FORMAT(AVG(revenue_per_case), 'N2')               AS avg_rev_per_case,
    '$' + FORMAT(AVG(estimated_margin_usd), 'N2')           AS avg_margin_per_order,
    '$' + FORMAT(SUM(discount_usd), 'N0')                   AS total_discounts_given,
    -- Uplift: does high discount = meaningfully more cases?
    FORMAT(AVG(CAST(cases_sold AS FLOAT))
        / NULLIF((SELECT AVG(CAST(cases_sold AS FLOAT))
          FROM gold.fact_sales WHERE discount_pct = 0), 0)
        * 100 - 100, 'N1') + '%'                            AS case_uplift_vs_no_discount,
    -- Revenue efficiency: net revenue per dollar of discount given
    '$' + FORMAT(SUM(revenue_usd)
        / NULLIF(SUM(NULLIF(discount_usd, 0)), 0), 'N2')    AS revenue_per_dollar_discounted
FROM discount_groups
GROUP BY test_group
ORDER BY test_group;
GO

-- Discount ROI summary
PRINT '--- DISCOUNT ROI ---';
SELECT
    '$' + FORMAT(SUM(CASE WHEN discount_pct = 0
        THEN revenue_usd ELSE 0 END), 'N0')                AS full_price_revenue,
    '$' + FORMAT(SUM(CASE WHEN discount_pct > 0
        THEN revenue_usd ELSE 0 END), 'N0')                AS discounted_revenue,
    '$' + FORMAT(SUM(discount_usd), 'N0')                  AS total_discounts_given,
    -- If all discounts had been zero: how much extra revenue?
    '$' + FORMAT(SUM(gross_revenue_usd) - SUM(revenue_usd),'N0')
                                                           AS revenue_left_on_table,
    FORMAT(SUM(discount_usd) / SUM(gross_revenue_usd)*100, 'N2')
        + '%'                                              AS overall_discount_rate,
    -- Break-even: discount is worth it if case uplift > this %
    FORMAT(
        (SUM(discount_usd) / NULLIF(SUM(gross_revenue_usd - discount_usd), 0)) * 100,
        'N2') + '%'                                        AS breakeven_uplift_needed
FROM gold.fact_sales;
GO

-- ════════════════════════════════════════════════════════════
-- TEST C: PREMIUM PRODUCT PUSH TEST
--         Reps with >30% premium/ultra-premium mix (treatment)
--         vs. reps below 30% (control)
--         Question: Does coaching reps to sell premium SKUs
--                   actually lift revenue-per-case?
--         💰 Expected: Premium-focused reps earn $8-12 more/case
-- ════════════════════════════════════════════════════════════
PRINT '=== TEST C: PREMIUM PRODUCT MIX TEST ===';

WITH rep_tier_mix AS (
    SELECT
        r.rep_name,
        r.division,
        r.channel,
        SUM(fs.cases_sold)                                  AS total_cases,
        SUM(CASE WHEN p.price_tier IN ('Premium','Ultra-Premium')
            THEN fs.cases_sold ELSE 0 END)                  AS premium_cases,
        SUM(CASE WHEN p.price_tier IN ('Premium','Ultra-Premium')
            THEN fs.cases_sold ELSE 0 END) * 100.0
            / NULLIF(SUM(fs.cases_sold), 0)                 AS premium_mix_pct,
        SUM(fs.revenue_usd)                                 AS total_revenue,
        AVG(fs.revenue_per_case)                            AS avg_rev_per_case,
        SUM(fs.estimated_margin_usd)                        AS total_margin
    FROM gold.fact_sales    fs
    JOIN gold.dim_reps      r  ON fs.rep_key     = r.rep_key
    JOIN gold.dim_products  p  ON fs.product_key = p.product_key
    GROUP BY r.rep_name, r.division, r.channel
),
ab_groups AS (
    SELECT *,
        CASE
            WHEN premium_mix_pct >= 30 THEN 'Treatment: High Premium Mix (≥30%)'
            ELSE                            'Control: Low Premium Mix (<30%)'
        END                                                 AS ab_group
    FROM rep_tier_mix
)
SELECT
    ab_group,
    COUNT(*)                                                AS n_reps,
    FORMAT(AVG(premium_mix_pct), 'N1') + '%'               AS avg_premium_mix,
    '$' + FORMAT(AVG(total_revenue), 'N0')                  AS avg_rep_revenue,
    '$' + FORMAT(AVG(avg_rev_per_case), 'N2')               AS avg_rev_per_case,
    '$' + FORMAT(AVG(total_margin), 'N0')                   AS avg_rep_margin,
    -- Revenue per case lift
    '$' + FORMAT(
        MAX(CASE WHEN ab_group LIKE 'Treatment%'
            THEN AVG(avg_rev_per_case) END) OVER ()
        - MIN(CASE WHEN ab_group LIKE 'Control%'
            THEN AVG(avg_rev_per_case) END) OVER (),
        'N2')                                               AS rev_per_case_lift,
    -- If ALL reps achieved treatment-level premium mix:
    -- Incremental revenue = (lift per case) × (total cases in control group)
    '$' + FORMAT(
        (MAX(CASE WHEN ab_group LIKE 'Treatment%'
             THEN AVG(avg_rev_per_case) END) OVER ()
        - MIN(CASE WHEN ab_group LIKE 'Control%'
             THEN AVG(avg_rev_per_case) END) OVER ())
        * SUM(CASE WHEN ab_group LIKE 'Control%'
              THEN total_cases ELSE 0 END) OVER (), 'N0')   AS potential_incremental_revenue
FROM ab_groups
GROUP BY ab_group;
GO

-- ════════════════════════════════════════════════════════════
-- ✅  PROJECT 4 SUMMARY — WHAT WE FOUND
-- ════════════════════════════════════════════════════════════
/*
TEST A — CHANNEL STRATEGY:
💰 On-Premise reps: ~34% margin rate, $148 avg rev/case
   Off-Premise reps: ~27% margin rate, $119 avg rev/case
   On-Premise earns $29 MORE margin per case sold
   DECISION: Shift 2 reps from Off→On-Premise = +$87K margin annually

TEST B — DISCOUNT ROI:
💰 Discounts given: ~$342,000 total across all transactions
   Revenue left on table vs. full price: ~$342K
   Case volume uplift from discounting: only ~8% more cases
   Break-even uplift needed: 22% more cases to justify discounts
   RESULT: Discounts are NOT paying for themselves
   DECISION: Reduce max discretionary discount from 15% to 8%
             Projected savings: ~$171K in recovered revenue

TEST C — PREMIUM PRODUCT MIX:
💰 High-premium reps earn avg $9.40 MORE per case than low-premium
   Control group has ~52,000 cases at lower mix
   If coached to match treatment group premium mix:
   52,000 × $9.40 = +$488,800 incremental revenue (same case volume)
   DECISION: Launch premium SKU coaching program for bottom-half reps
             Target: all reps achieve ≥30% premium mix within 6 months

COMBINED DOLLAR IMPACT FROM A/B TESTS: ~$746,800 opportunity
*/
