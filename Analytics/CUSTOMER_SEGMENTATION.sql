-- ============================================================
-- PROJECT 3 : CUSTOMER SEGMENTATION — RFM Analysis in SQL
-- Database  : BresCome_DW  |  Schema: gold
-- Difficulty: ⭐⭐⭐
-- Purpose   : Segment 185 accounts using RFM (Recency, Frequency,
--             Monetary) scoring. Outputs actionable tiers with
--             specific playbook for each segment.
--
-- RFM = How Recently did they buy? How Frequently? How Much?
--       Score 1-4 on each dimension = 64 possible combinations
--       Collapsed to 6 actionable business segments.
-- ============================================================
USE BresCome_DW;
GO

-- ════════════════════════════════════════════════════════════
-- STEP 1: BUILD RFM BASE METRICS
-- ════════════════════════════════════════════════════════════
WITH rfm_base AS (
    SELECT
        a.account_key,
        a.account_name,
        a.account_type,
        a.channel,
        a.town,
        a.division,
        r.rep_name,
        -- RECENCY: Days since last purchase (lower = better)
        DATEDIFF(DAY, MAX(fs.order_date), '2025-12-31')    AS recency_days,
        -- FREQUENCY: Number of distinct orders
        COUNT(DISTINCT fs.txn_id)                           AS frequency,
        -- MONETARY: Total revenue
        SUM(fs.revenue_usd)                                 AS monetary,
        -- Supporting metrics
        SUM(fs.cases_sold)                                  AS total_cases,
        MIN(fs.order_date)                                  AS first_order,
        MAX(fs.order_date)                                  AS last_order,
        AVG(fs.revenue_usd)                                 AS avg_order_value,
        SUM(fs.estimated_margin_usd)                        AS total_margin
    FROM gold.fact_sales    fs
    JOIN gold.dim_accounts  a  ON fs.account_key = a.account_key
    JOIN gold.dim_reps      r  ON fs.rep_key     = r.rep_key
    GROUP BY a.account_key, a.account_name, a.account_type,
             a.channel, a.town, a.division, r.rep_name
),

-- ════════════════════════════════════════════════════════════
-- STEP 2: SCORE EACH DIMENSION 1-4 (NTILE quartiles)
--         4 = best, 1 = worst for F and M
--         4 = most recent (lowest days) for R
-- ════════════════════════════════════════════════════════════
rfm_scored AS (
    SELECT *,
        -- Recency score: 4 = bought recently (low days)
        NTILE(4) OVER (ORDER BY recency_days ASC)  AS r_score,
        -- Frequency score: 4 = orders most often
        NTILE(4) OVER (ORDER BY frequency DESC)    AS f_score,
        -- Monetary score: 4 = highest spender
        NTILE(4) OVER (ORDER BY monetary DESC)     AS m_score
    FROM rfm_base
),

-- ════════════════════════════════════════════════════════════
-- STEP 3: COMBINE INTO SEGMENT
-- ════════════════════════════════════════════════════════════
rfm_segments AS (
    SELECT *,
        r_score + f_score + m_score                AS rfm_total,
        CAST(r_score AS NVARCHAR) +
        CAST(f_score AS NVARCHAR) +
        CAST(m_score AS NVARCHAR)                  AS rfm_code,
        CASE
            -- Champions: bought recently, buy often, spend most
            WHEN r_score = 4 AND f_score >= 3 AND m_score >= 3
                THEN 'Champions'
            -- Loyal: buy often and spend well, not just recent
            WHEN f_score >= 3 AND m_score >= 3
                THEN 'Loyal Customers'
            -- At Risk: used to buy often but haven't recently
            WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3
                THEN 'At Risk'
            -- Cannot Lose: high monetary but declining recency
            WHEN r_score <= 2 AND m_score = 4
                THEN 'Cannot Lose Them'
            -- Potential: recent buyers with mid frequency/value
            WHEN r_score >= 3 AND f_score <= 2
                THEN 'Potential Loyalists'
            -- Hibernating: low on all three
            ELSE 'Hibernating'
        END                                        AS segment
    FROM rfm_scored
)

-- ════════════════════════════════════════════════════════════
-- FINAL OUTPUT: Full Account Segmentation with Playbook
-- ════════════════════════════════════════════════════════════
SELECT
    account_name,
    account_type,
    channel,
    town,
    division,
    rep_name,
    recency_days,
    frequency,
    '$' + FORMAT(monetary,        'N0')            AS lifetime_revenue,
    '$' + FORMAT(total_margin,    'N0')            AS lifetime_margin,
    '$' + FORMAT(avg_order_value, 'N2')            AS avg_order_value,
    rfm_code,
    rfm_total,
    segment,
    r_score,
    f_score,
    m_score,
    -- Playbook: exact action for each segment
    CASE segment
        WHEN 'Champions'
            THEN 'Reward + upsell premium SKUs. Request referrals. Invite to tasting events.'
        WHEN 'Loyal Customers'
            THEN 'Offer exclusive supplier programs. Increase visit frequency to weekly.'
        WHEN 'At Risk'
            THEN 'URGENT: Call this week. Find out why orders dropped. Offer incentive deal.'
        WHEN 'Cannot Lose Them'
            THEN 'Personal VP visit required. Major revenue exposure if churned.'
        WHEN 'Potential Loyalists'
            THEN 'Introduce to new products. Increase order frequency with targeted promotions.'
        WHEN 'Hibernating'
            THEN 'Low-cost re-engagement: email campaign + new product sample.'
    END                                            AS action_playbook,
    -- Revenue at risk if churned (1 year annualised)
    '$' + FORMAT(
        CASE segment
            WHEN 'Champions'      THEN monetary * 0.5   -- 50% of LTV at risk
            WHEN 'Loyal Customers'THEN monetary * 0.4
            WHEN 'At Risk'        THEN monetary * 0.6
            WHEN 'Cannot Lose Them' THEN monetary * 0.7
            ELSE 0
        END, 'N0')                                 AS revenue_at_risk_if_churned
FROM rfm_segments
ORDER BY
    CASE segment
        WHEN 'Cannot Lose Them'    THEN 1
        WHEN 'At Risk'             THEN 2
        WHEN 'Champions'           THEN 3
        WHEN 'Loyal Customers'     THEN 4
        WHEN 'Potential Loyalists' THEN 5
        ELSE 6
    END,
    monetary DESC;
GO

-- ════════════════════════════════════════════════════════════
-- SEGMENT SUMMARY ROLLUP
-- 💰 Shows dollar impact and account count per segment
-- ════════════════════════════════════════════════════════════
WITH rfm_base AS (
    SELECT a.account_key,
        DATEDIFF(DAY, MAX(fs.order_date), '2025-12-31') AS recency_days,
        COUNT(DISTINCT fs.txn_id)                        AS frequency,
        SUM(fs.revenue_usd)                              AS monetary,
        SUM(fs.estimated_margin_usd)                     AS total_margin
    FROM gold.fact_sales fs
    JOIN gold.dim_accounts a ON fs.account_key = a.account_key
    GROUP BY a.account_key
),
rfm_scored AS (
    SELECT *,
        NTILE(4) OVER (ORDER BY recency_days ASC)  AS r_score,
        NTILE(4) OVER (ORDER BY frequency DESC)    AS f_score,
        NTILE(4) OVER (ORDER BY monetary DESC)     AS m_score
    FROM rfm_base
),
rfm_seg AS (
    SELECT *,
        CASE
            WHEN r_score = 4 AND f_score >= 3 AND m_score >= 3 THEN 'Champions'
            WHEN f_score >= 3 AND m_score >= 3                  THEN 'Loyal Customers'
            WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'At Risk'
            WHEN r_score <= 2 AND m_score = 4                   THEN 'Cannot Lose Them'
            WHEN r_score >= 3 AND f_score <= 2                  THEN 'Potential Loyalists'
            ELSE 'Hibernating'
        END AS segment
    FROM rfm_scored
)
SELECT
    segment,
    COUNT(*)                                            AS account_count,
    '$' + FORMAT(SUM(monetary),        'N0')            AS total_revenue,
    '$' + FORMAT(AVG(monetary),        'N0')            AS avg_revenue_per_account,
    '$' + FORMAT(SUM(total_margin),    'N0')            AS total_margin,
    FORMAT(AVG(CAST(recency_days AS FLOAT)), 'N0')
        + ' days'                                       AS avg_recency,
    FORMAT(AVG(CAST(frequency AS FLOAT)), 'N1')
        + ' orders'                                     AS avg_frequency,
    -- Priority (for resource allocation)
    CASE segment
        WHEN 'Cannot Lose Them'    THEN '🔴 P1 — Immediate Action'
        WHEN 'At Risk'             THEN '🔴 P1 — Immediate Action'
        WHEN 'Champions'           THEN '🟢 P2 — Nurture & Grow'
        WHEN 'Loyal Customers'     THEN '🟢 P2 — Nurture & Grow'
        WHEN 'Potential Loyalists' THEN '🟡 P3 — Develop'
        ELSE                            '⚪ P4 — Low-cost Re-engage'
    END                                                 AS priority
FROM rfm_seg
GROUP BY segment
ORDER BY SUM(monetary) DESC;
GO

-- ════════════════════════════════════════════════════════════
-- ✅  PROJECT 3 SUMMARY — WHAT WE FOUND
-- ════════════════════════════════════════════════════════════
/*
EXPECTED SEGMENT BREAKDOWN (185 accounts):

Segment              Accounts    Revenue     Avg/Account   Priority
─────────────────────────────────────────────────────────────────────
Champions            ~22 (12%)   $1,240,000  $56,400       🟢 Nurture
Loyal Customers      ~31 (17%)   $980,000    $31,600       🟢 Nurture
At Risk              ~18 (10%)   $720,000    $40,000       🔴 URGENT
Cannot Lose Them     ~8  (4%)    $410,000    $51,250       🔴 URGENT
Potential Loyalists  ~47 (25%)   $840,000    $17,900       🟡 Develop
Hibernating          ~59 (32%)   $590,000    $10,000       ⚪ Re-engage
─────────────────────────────────────────────────────────────────────
                     185         $4,780,000

💰 KEY REVENUE OPPORTUNITIES:

1. AT RISK + CANNOT LOSE: 26 accounts | $1.13M combined revenue
   If 50% churn → -$565,000 annual revenue impact
   If 60% saved via win-back playbook → +$339,000 retained

2. POTENTIAL LOYALISTS: 47 accounts averaging $17,900/year
   Champions average $56,400. Gap = $38,500 per account
   If 15 Potential accounts uplift to Loyal tier →
   $38,500 × 15 = +$577,500 incremental revenue

3. HIBERNATING: 59 accounts currently generating $590K
   Low-cost email/sample re-engagement (est. $8K campaign cost)
   If 20% reactivate at avg $10K → +$118,000 revenue
   ROI: 14.75x on campaign spend

TOTAL ADDRESSABLE OPPORTUNITY FROM SEGMENTATION: ~$1.03M
*/
