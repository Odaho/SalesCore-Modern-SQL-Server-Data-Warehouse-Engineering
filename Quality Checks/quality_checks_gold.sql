/*
================================================================================
GOLD LAYER DATA QUALITY & MODEL VALIDATION SCRIPT
Layer: Gold (Business-Ready Dimensional Model)

Purpose:
    Validate dimensional model integrity after transformation
    from Silver → Gold layer.

Focus Areas:
    - Surrogate key uniqueness
    - Referential integrity between fact and dimension tables
    - Star schema connectivity validation

Expected Result:
    Queries should return ZERO rows.
    Any returned records indicate a dimensional model integrity issue.
================================================================================
*/


/* =============================================================================
   VALIDATION: gold.dim_customers
============================================================================= */

-- ---------------------------------------------------------------------------
-- Check 1: Surrogate Key Uniqueness
-- Business Rule:
--     Each customer_key must be unique within the dimension table.
-- Why This Matters:
--     Duplicate surrogate keys break fact-to-dimension joins.
-- Expectation:
--     No duplicate keys.
-- ---------------------------------------------------------------------------
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;



/* =============================================================================
   VALIDATION: gold.dim_products
============================================================================= */

-- ---------------------------------------------------------------------------
-- Check 1: Surrogate Key Uniqueness
-- Business Rule:
--     Each product_key must be unique within the dimension table.
-- Why This Matters:
--     Ensures one-to-one mapping between product dimension
--     and fact table references.
-- Expectation:
--     No duplicate keys.
-- ---------------------------------------------------------------------------
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;



/* =============================================================================
   VALIDATION: gold.fact_sales
============================================================================= */

-- ---------------------------------------------------------------------------
-- Check 1: Referential Integrity Validation
-- Business Rule:
--     Every foreign key in fact_sales must match a valid
--     record in its respective dimension table.
--
-- Why This Matters:
--     Orphaned records break analytical queries and aggregations.
--     Star schema integrity depends on proper key relationships.
--
-- Expectation:
--     No rows returned.
-- ---------------------------------------------------------------------------
SELECT 
    f.*
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL 
   OR c.customer_key IS NULL;
