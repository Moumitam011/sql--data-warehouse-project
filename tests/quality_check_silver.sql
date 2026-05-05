/*
===========================================================
Quality Checks
===========================================================

Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===========================================================
*/
--===============================================
-- 1.            silver.crm_cust_info
--===============================================
-- CHECK FOR NULSS AND DUPLICATES IN PRIMARY KEY
--EXPECTATION: NO RESULT

SELECT cst_id,
count(*) from silver.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 or cst_id IS NULL;


-- CHECK FOR UNWANTED SPACE IN STRING
--EXPECTATION: NO RESULT
SELECT cst_firstname
FROM silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)


--DATA STANDARIZATION & CONSISTANCY
SELECT  distinct cst_marital_status from  silver.crm_cust_info;
SELECT  distinct cst_gndr from  silver.crm_cust_info;

go


--===============================================
-- 2.            silver.crm_prd_info
--===============================================
-- CHECK FOR NULSS AND DUPLICATES IN PRIMARY KEY
--EXPECTATION: NO RESULT

SELECT prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
HAVING count(*) > 1 OR prd_id is Null -- evrything is okay 


--prd_key = divide it using substring and make it 2 column
SELECT prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key
from silver.crm_prd_info

--prd_nm
-- CHECK FOR UNWANTED SPACE IN STRING
--EXPECTATION: NO RESULT
SELECT prd_nm
from silver.crm_prd_info
WHERE prd_nm !=  TRIM(prd_nm) -- SAFE

--prd_cost
--check for nulls and negetive numbers
SELECT prd_cost from silver.crm_prd_info
where prd_cost is Null or prd_cost <0 -- Handle the null with 0

--prd_line
-- Data standarization and consistancy (abbreviation)
SELECT CASE UPPER(TRIM(prd_line))
     WHEN  'M' THEN 'Mountain'
     WHEN  'R' THEN 'Road'
     WHEN  'S' THEN 'Other sales'
     WHEN  'T' THEN 'Touring'
     ELSE 'n/a'
END prd_line
from silver.crm_prd_info

select distinct prd_line from silver.crm_prd_info

--prd_start dt & prd_end_dt
--CHECK FOR INVALID DATE ORDERS
--END DATE MUST NOT BE EALIER THAN FIRST DATE

 SELECT 
 prd_start_dt,
 prd_end_dt  ,
 DATEADD(DAY,-1,
         LEAD(prd_start_dt) over(
         partition by prd_key 
         order by prd_start_dt desc)) as prd_end_dt_test
 from silver.crm_prd_info

select * from silver.crm_prd_info
where prd_end_dt < prd_start_dt

go

--===============================================
-- 3.            silver.crm_sales_details
--===============================================
-- QUALITY CHECK 1
-- CHECK FOR NULSS AND DUPLICATES IN PRIMARY KEY
--EXPECTATION: NO RESULT

--QUALITY CHECK 2
-- check 1st column has any unwanted spaces or not as its a string
SELECT 
sls_ord_num
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM( sls_ord_num) -- EVERYTHING IS FINE (no spaces)

-- QUALITY CHECK 3
-- check 2nd column sls_prd_key keys can be used and connected with the silver.crm_prd_info
SELECT 
sls_prd_key
FROM silver.crm_sales_details
WHERE sls_prd_key not in (SELECT prd_key from silver.crm_prd_info)

-- QUALITY CHECK 4
---- check 3rd column sls_cust_id keys can be used and connected with the silver.crm_cust_info
SELECT 
sls_cust_id
FROM silver.crm_sales_details
WHERE sls_cust_id not in (SELECT cst_id from silver.crm_cust_info)

--QUALITY CHECK 5
--CHECK for invalid dates (int to date)
--negetive number or zeros cant be cast as date
SELECT 
NULLIF (sls_order_dt, 0) AS sls_order_dt -- Handle 0 to null
from silver.crm_sales_details
WHERE sls_order_dt <= 0 -- here we are getting lot of zeros change 0 to null using nullif
or len(sls_order_dt) != 8 -- 20101229 (8) char check if all values have 8 character
or sls_order_dt > 20500101 -- higher boundary
or sls_order_dt < 19000101  -- lowest boundary -- we are getting issues like all of these


-- QUALITY CHECK 6  - SAME FOR  sls_ship_dt
SELECT 
NULLIF (sls_ship_dt, 0) as sls_ship_dt -- Handle 0 to null
from silver.crm_sales_details
WHERE sls_ship_dt <= 0 -- here we are getting lot of zeros change 0 to null using nullif
or len(sls_ship_dt) != 8 -- 20101229 (8) char check if all values have 8 character
or sls_ship_dt > 20500101 -- higher boundary
or sls_ship_dt < 19000101  -- lowest boundary -- we are getting issues like all of these

-- there is no issue in sls_ship_dt but we will transform too for future


-- QUALITY CHECK 7 - SAME FOR  sls_due_dt
SELECT 
NULLIF (sls_due_dt, 0) as sls_due_dt -- Handle 0 to null
from silver.crm_sales_details
WHERE sls_due_dt <= 0 -- here we are getting lot of zeros change 0 to null using COALESCE
or len(sls_due_dt) != 8 -- 20101229 (8) char check if all values have 8 character
or sls_due_dt > 20500101 -- higher boundary
or sls_due_dt < 19000101  -- lowest boundary -- we are getting issues like all of these

-- there is no issue in sls_due_dt but we will transform too for future


-- QUALITY CHECK 8 - orderdate should be always smaller than shipping date or due date
SELECT * 
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_ship_dt

-- QUALITY CHECK 9 
-- sales = quantity * price
-- negetive , zeros and NULLS are not allowed

SELECT distinct
sls_sales ,
sls_quantity,
sls_price
FROM silver.crm_sales_details
where sls_sales != sls_quantity * sls_price -- where business rules expectation did not meet
or sls_sales is null or sls_quantity is null or sls_price is null -- check for the null values
or sls_sales <= 0 or  sls_quantity  <= 0 or sls_price  <= 0  -- check for negetive numbers
order by sls_sales, sls_quantity, sls_price --ALL THE neg, zero and nulls are present


--Handle zeros nulls and negetive
-- RULES
--if sales is negetive or zero then calculate it from quantity ans price
--if price is  null or zero calculate it from sales and quantity
--if price is negetive then convert it into a positive value
SELECT distinct
sls_sales AS old_sls_sales ,
sls_quantity,
sls_price as old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
     THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
END as sls_sales,

CASE WHEN sls_price IS NULL OR sls_price<=0 
        THEN sls_sales / NULLIF(sls_quantity,0)
     ELSE sls_price  
END AS sls_price
FROM silver.crm_sales_details
where sls_sales != sls_quantity * sls_price -- where business rules expectation did not meet
or sls_sales is null or sls_quantity is null or sls_price is null -- check for the null values
or sls_sales <= 0 or  sls_quantity  <= 0 or sls_price  <= 0  -- check for negetive numbers
order by sls_sales, sls_quantity, sls_price



-- SILVER ERP DATA HANDELLING 
-- ============================
--        erp_cust_az12
-- ============================
SELECT 
cid,
CASE WHEN cid  LIKE 'NAS%' THEN  SUBSTRING(cid,4,len(cid))
     ELSE cid
END as cid,
bdate,
CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate
END as  bdate ,
CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     ELSE 'n/a'
END AS gen
from silver.erp_cust_az12


--QUALITY CHECK 1 - Handle the cid where extra character are present
SELECT 
cid,
CASE WHEN cid  LIKE 'NAS%' THEN  SUBSTRING(cid,4,len(cid))
     ELSE cid
END as cid
from silver.erp_cust_az12

--QUALITY CHECK 2 - IDENTIFY OUT OF RANGE DATES
SELECT bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' OR bdate > getdate()

--QUALITY CHECK 3 - check the gen column has all the correct values
SELECT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     ELSE 'n/a'
END AS gen
from silver.erp_cust_az12

 
 GO


-- ============================
--         erp_loc_a101 
-- ============================
SELECT 
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry) = '' OR cntry = NULL THEN 'n/a'
     ELSE TRIM(cntry)
END AS cntry
from silver.erp_loc_a101

-- QUALITY CHECK 1 --
SELECT 
REPLACE(cid, '-', '') cid
from silver.erp_loc_a101

-- QUALITY CHECK 2 -- DATA STANDARIZATION AND CONSISTANCY
 

SELECT 
cntry ,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
     ELSE TRIM(cntry)
END AS cntry
from silver.erp_loc_a101
go


-- ============================
--         erp_px_cat_g1v2 
-- ============================
SELECT 
id,
cat,
subcat,
maintenance
from silver.erp_px_cat_g1v2

-- QUALITY CHECK 1 -- already sorted in product details crm

-- QUALITY CHECK 2 -- 2nd - to last col Check unwanted string values or extra spaces
SELECT
*
from silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) -- EVERYTHING IS SAFE
OR subcat != TRIM(subcat)
OR maintenance != TRIM(maintenance)

-- QUALITY CHECK 3 
--DATA STANDARIZATION AND CONSISTANCY
SELECT DISTINCT 
maintenance
from silver.erp_px_cat_g1v2
