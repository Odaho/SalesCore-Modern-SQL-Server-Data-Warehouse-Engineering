/*
================================================================================
SILVER LAYER DATA QUALITY VALIDATION SCRIPT
Layer: Silver (Cleansed & Standardized Layer)

Purpose:
    Validate data integrity, consistency, and business rule compliance
    after transformation from Bronze → Silver.

Expected Result:
    Most queries should return ZERO rows.
    Any returned rows indicate data quality issues requiring investigation.
================================================================================
*/


/* =============================================================================
   VALIDATION: silver.crm_cust_info
============================================================================= */

-- ---------------------------------------------------------------------------
-- Check 1: Primary Key Integrity
-- Business Rule:
--     Each customer (cst_id) must be unique and not NULL.
-- Expectation:
--     No duplicate or NULL primary keys.
-- ---------------------------------------------------------------------------
SELECT 
    cst_id,
    COUNT(*) AS record_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- ---------------------------------------------------------------------------
-- Check 2: Whitespace Normalization
-- Business Rule:
--     No leading or trailing spaces in key fields.
-- Expectation:
--     No rows returned.
-- ---------------------------------------------------------------------------
SELECT 
    cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);


-- ---------------------------------------------------------------------------
-- Check 3: Marital Status Standardization
-- Business Rule:
--     Only standardized values should exist (Single, Married, n/a).
-- ---------------------------------------------------------------------------
SELECT DISTINCT 
    cst_marital_status
FROM silver.crm_cust_info;



/* =============================================================================
   VALIDATION: silver.crm_prd_info
============================================================================= */

-- ---------------------------------------------------------------------------
-- Check 1: Primary Key Integrity
-- Business Rule:
--     Product ID must be unique and not NULL.
-- ---------------------------------------------------------------------------
SELECT 
    prd_id,
    COUNT(*) AS record_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- ---------------------------------------------------------------------------
-- Check 2: Whitespace Normalization (Product Name)
-- ---------------------------------------------------------------------------
SELECT 
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- ---------------------------------------------------------------------------
-- Check 3: Cost Validation
-- Business Rule:
--     Product cost must not be NULL or negative.
-- ---------------------------------------------------------------------------
SELECT 
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


-- ---------------------------------------------------------------------------
-- Check 4: Product Line Standardization
-- Expected values: Mountain, Road, Touring, Other Sales, n/a
-- ---------------------------------------------------------------------------
SELECT DISTINCT 
    prd_line
FROM silver.crm_prd_info;


-- ---------------------------------------------------------------------------
-- Check 5: Date Range Integrity
-- Business Rule:
--     Product end date must not precede start date.
-- ---------------------------------------------------------------------------
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;



/* =============================================================================
   VALIDATION: silver.crm_sales_details
============================================================================= */

-- ---------------------------------------------------------------------------
-- Check 1: Raw Date Validation (Bronze Layer Sanity Check)
-- Business Rule:
--     Dates must be valid YYYYMMDD values within reasonable range.
-- ---------------------------------------------------------------------------
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
   OR LEN(sls_due_dt) != 8 
   OR sls_due_dt > 20500101 
   OR sls_due_dt < 19000101;


-- ---------------------------------------------------------------------------
-- Check 2: Logical Date Order Validation
-- Business Rule:
--     Order date must not be after ship or due date.
-- ---------------------------------------------------------------------------
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;


-- ---------------------------------------------------------------------------
-- Check 3: Sales Calculation Integrity
-- Business Rule:
--     Sales must equal Quantity * Price.
--     All numeric fields must be positive and non-null.
-- ---------------------------------------------------------------------------
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;



/* =============================================================================
   VALIDATION: silver.erp_cust_az12
============================================================================= */

-- ---------------------------------------------------------------------------
-- Check 1: Birthdate Range Validation
-- Business Rule:
--     Birthdates must be realistic.
--     Range enforced: 1924-01-01 to current date.
-- ---------------------------------------------------------------------------
SELECT DISTINCT 
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();


-- ---------------------------------------------------------------------------
-- Check 2: Gender Standardization
-- Expected values: Male, Female, n/a
-- ---------------------------------------------------------------------------
SELECT DISTINCT 
    gen
FROM silver.erp_cust_az12;



/* =============================================================================
   VALIDATION: silver.erp_loc_a101
============================================================================= */

-- ---------------------------------------------------------------------------
-- Check: Country Standardization
-- Ensures normalization logic applied correctly.
-- ---------------------------------------------------------------------------
SELECT DISTINCT 
    cntry
FROM silver.erp_loc_a101
ORDER BY cntry;



/* =============================================================================
   VALIDATION: silver.erp_px_cat_g1v2
============================================================================= */

-- ---------------------------------------------------------------------------
-- Check 1: Whitespace Normalization
-- ---------------------------------------------------------------------------
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);


-- ---------------------------------------------------------------------------
-- Check 2: Maintenance Value Standardization
-- ---------------------------------------------------------------------------
SELECT DISTINCT 
    maintenance
FROM silver.erp_px_cat_g1v2;
