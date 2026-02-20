/*
================================================================================
GOLD LAYER – DIMENSIONAL MODEL (STAR SCHEMA)
Layer: Gold (Business-Ready Reporting Layer)

Purpose:
    - Transform cleansed Silver data into a dimensional model
    - Expose business-friendly views for analytics and BI
    - Implement surrogate keys for star schema relationships

Design Pattern:
    Star Schema
        - Dimensions: dim_customers, dim_products
        - Fact: fact_sales

Note:
    Surrogate keys are generated using ROW_NUMBER().
    (For production systems, identity columns or persisted keys are preferred.)
================================================================================
*/


/* =============================================================================
   DIMENSION: gold.dim_customers
============================================================================= */

-- ---------------------------------------------------------------------------
-- Business Purpose:
--     Consolidated customer dimension integrating CRM (primary)
--     and ERP (enrichment attributes).
--
-- Grain:
--     One row per unique customer.
--
-- Key Strategy:
--     Surrogate key (customer_key) generated via ROW_NUMBER().
-- ---------------------------------------------------------------------------

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    /* Surrogate Key for Star Schema joins */
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,

    /* Business/Natural Keys */
    ci.cst_id        AS customer_id,
    ci.cst_key       AS customer_number,

    /* Descriptive Attributes */
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,
    la.cntry         AS country,
    ci.cst_marital_status AS marital_status,

    /* Gender Prioritization Logic:
       CRM is primary source.
       ERP used only when CRM value = 'n/a'. */
    CASE 
        WHEN ci.cst_gndr != 'n/a' 
            THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,

    /* Enrichment Attributes from ERP */
    ca.bdate         AS birthdate,

    /* Metadata */
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;

GO



/* =============================================================================
   DIMENSION: gold.dim_products
============================================================================= */

-- ---------------------------------------------------------------------------
-- Business Purpose:
--     Product dimension enriched with category hierarchy.
--
-- Grain:
--     One row per active product (current version only).
--
-- Filtering Logic:
--     Only current records retained (prd_end_dt IS NULL).
--
-- Key Strategy:
--     Surrogate key generated via ROW_NUMBER().
-- ---------------------------------------------------------------------------

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    /* Surrogate Key */
    ROW_NUMBER() OVER (
        ORDER BY pn.prd_start_dt, pn.prd_key
    ) AS product_key,

    /* Natural Keys */
    pn.prd_id  AS product_id,
    pn.prd_key AS product_number,

    /* Descriptive Attributes */
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id

/* Retain only active/current products */
WHERE pn.prd_end_dt IS NULL;

GO



/* =============================================================================
   FACT TABLE: gold.fact_sales
============================================================================= */

-- ---------------------------------------------------------------------------
-- Business Purpose:
--     Central transactional fact table capturing sales activity.
--
-- Grain:
--     One row per sales order line.
--
-- Foreign Keys:
--     product_key → dim_products
--     customer_key → dim_customers
--
-- Measures:
--     sales_amount
--     quantity
--     price
--
-- Notes:
--     LEFT JOIN used to detect orphaned records during validation.
--     Referential integrity validated via QA scripts.
-- ---------------------------------------------------------------------------

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    /* Degenerate Dimension */
    sd.sls_ord_num AS order_number,

    /* Foreign Keys */
    pr.product_key,
    cu.customer_key,

    /* Date Attributes */
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,

    /* Measures */
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price

FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;

GO

