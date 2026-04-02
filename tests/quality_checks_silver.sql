/*

All the data cleaning in the silver layer

*/

-- Check for nulls or duplicates in primary key
-- Expectation: no results

SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Only selecting the last of the duplicates in primary key:

SELECT *
FROM(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
FROM bronze.crm_cust_info)t WHERE flag_last = 1 

-- Check for unwanted spaces

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- Data standardization & consistency

SELECT DISTINCT cst_gndr,
    CASE WHEN cst_gndr = 'F' THEN 'Female'
        WHEN cst_gndr = 'M' THEN 'Male'
    END
FROM bronze.crm_cust_info

-- Find Category that is not in bronze.erp_px_cat_g1v2

SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') NOT IN
(SELECT DISTINCT id from bronze.erp_px_cat_g1v2)

-- Check if table can be connected with silver.crm_prd_info

SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_sales,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN
(
    SELECT prd_key
    FROM silver.crm_prd_info
)

-- Convert sls_order_dt, sls_ship_dt, sls_due_dt into dates

SELECT
    NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt = 0 OR LEN(sls_order_dt) != 8

-- Check the quality of data: sales, quantity and price

SELECT DISTINCT
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    
    
    CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price
    
    END sls_price    
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


-- Find cid not in silver.crm_cust_info

SELECT
    cid,
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
    END cid,
    bdate,
    gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
    END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- Match cid with cst_key from silver.crm_cust_info


SELECT 
    REPLACE(cid, '-','') cid,
    cntry
FROM bronze.erp_loc_a101 WHERE REPLACE(cid, '-','')  NOT IN (SELECT cst_key FROM silver.crm_cust_info)






