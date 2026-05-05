/*
====================================================================
Stored Procedure: Load silver Layer (bronze -> silver)
====================================================================

Script Purpose:
    This stored procedure performs the ETL process to populate the 'silver' 
    schema table from the 'bronze' schema.
    It performs the following actions:
    - Truncates the silver tables.
    -Insert transformed and cleaned data from bronze to silver table. 

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;

====================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
   DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
     BEGIN TRY 
    --CRM
         SET @batch_start_time = GETDATE()
         PRINT ('=======================================================')
	     PRINT ('LOADING  SILVER LAYER')
	     PRINT ('=======================================================')


		 PRINT ('-------------------------------------------------------');
		 PRINT ('loading CRM tables');
		 PRINT ('-------------------------------------------------------');		

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table : silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting Table : silver.crm_cust_info';

    INSERT INTO silver.crm_cust_info (
     cst_id,
     cst_key,
     cst_firstname, 
     cst_lastname, 
     cst_marital_status,
     cst_gndr,
     cst_create_date
    )
    SELECT 
	    cst_id,
	    cst_key, 
	    TRIM(cst_firstname) AS cst_firstname,
	    TRIM(cst_lastname) AS cst_lastname,
	    CASE 
		     WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	         ELSE 'n/a'
       END cst_marital_status,   
       CASE 
             WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	         ELSE 'n/a'
       END cst_gndr, 
       cst_create_date
    FROM ( 
        SELECT 
	        * ,
		    row_number() over(partition by cst_id order by cst_create_date desc) as  flag_last
	    FROM bronze.crm_cust_info
	    WHERE cst_id IS NOT NULL
     )t
     where flag_last = 1 ;

     SET @end_time = GETDATE()
     PRINT'>> Load duration : ' + cast (DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) +'second'
	 PRINT '---------------------' ;


    SET @start_time= GETDATE()
    PRINT '>> Truncating Table : silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    PRINT '>> Inserting Table : silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info (
    prd_id ,
    cat_id ,
    prd_key,
    prd_nm ,
    prd_cost,
    prd_line,
    prd_start_dt ,
    prd_end_dt 
    )
    SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, -- Extract category id
    SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,      -- Extract product id
    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,                  -- Handled null with 0
    CASE UPPER(TRIM(prd_line))
         WHEN  'M' THEN 'Mountain'
         WHEN  'R' THEN 'Road'
         WHEN  'S' THEN 'Other sales'
         WHEN  'T' THEN 'Touring'
         ELSE 'n/a'                                    --Normalise the column to readable format
    END prd_line,
    CAST (prd_start_dt AS DATE ) AS prd_start_dt  ,
     CAST (DATEADD(DAY,-1,
            LEAD(prd_start_dt) over(                     -- Data enrichment
             partition by prd_key                       -- add new , relevant data toenhance  the dataset for analysis     
             order by prd_start_dt) ) AS DATE) as prd_end_dt -- Calculate the end date as one day before the next start date
    from bronze.crm_prd_info;
    SET @end_time = GETDATE()
    PRINT'>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar ) + 'second'
    PRINT '---------------------' 

    SET @start_time= GETDATE()
    PRINT '>> Truncating Table : silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>> Inserting Table : silver.crm_sales_details';

    INSERT INTO  silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
    )

    SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt =0 OR LEN(sls_order_dt) !=8 THEN NULL
         ELSE CAST (CAST(sls_order_dt AS VARCHAR) AS DATE) --1st convert INT to varchar then varchar to date ad we cannot directly convert int to date 
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt =0 OR LEN(sls_ship_dt) !=8 THEN NULL
         ELSE CAST (CAST(sls_ship_dt AS VARCHAR) AS DATE) --1st convert INT to varchar then varchar to date ad we cannot directly convert int to date 
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt =0 OR LEN(sls_due_dt) !=8 THEN NULL
         ELSE CAST (CAST(sls_due_dt AS VARCHAR) AS DATE) --1st convert INT to varchar then varchar to date ad we cannot directly convert int to date 
    END as sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
         ELSE sls_sales
    END as sls_sales,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price<=0 
            THEN sls_sales / NULLIF(sls_quantity,0)
         ELSE sls_price  
    END AS sls_price
    FROM bronze.crm_sales_details;
    SET @end_time = GETDATE()
    PRINT'>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar ) + 'second'
    PRINT '---------------------' 

    -- ERP

    	PRINT ('-------------------------------------------------------');
		PRINT ('loading ERP tables');
		PRINT ('-------------------------------------------------------');		
    SET @start_time =GETDATE()
    PRINT '>> Truncating Table : silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT '>> Inserting Table : silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
    )
    SELECT 
    CASE WHEN cid  LIKE 'NAS%' THEN  SUBSTRING(cid,4,len(cid)) -- remove 'NAS' prefix if present
         ELSE cid
    END as cid,
    CASE WHEN bdate > GETDATE() THEN NULL -- set future boundaries to null
         ELSE bdate
    END as  bdate ,
    CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
         WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
         ELSE 'n/a' -- normalise gender values and handle unknown cases
    END AS gen
    from bronze.erp_cust_az12
    SET @end_time = GETDATE()
    PRINT'>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar ) + 'second'
    PRINT '---------------------' 

    SET @start_time = GETDATE()
    PRINT '>> Truncating Table : silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    PRINT '>> Inserting Table : silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry)

    SELECT 
    REPLACE(cid, '-', '') cid,  -- Handled invalid values
    CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
         WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
         WHEN TRIM(cntry) = '' OR cntry IS  NULL THEN 'n/a'
         ELSE TRIM(cntry)
    END AS cntry   -- Normalize and handle missing or blank country codes
    from bronze.erp_loc_a101
    SET @end_time = GETDATE()
    PRINT'>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar ) + 'second'
    PRINT '---------------------' 

    SET @start_time= GETDATE()
    PRINT '>> Truncating Table : silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    PRINT '>> Inserting Table : silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2
    (
    id,
    cat,
    subcat,
    maintenance
    )
    SELECT 
    id,
    cat,
    subcat,
    maintenance
    from bronze.erp_px_cat_g1v2;
    SET @end_time = GETDATE()
    PRINT'>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar ) + 'second'
    PRINT '---------------------' 

    SET @batch_end_time = GETDATE()
    PRINT '=============================='
    PRINT '>> Batch loading duration : ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS nvarchar) + 'second'
    PRINT '=============================='
END TRY 
BEGIN CATCH 
    PRINT 'ERROR OCCURED during loading the silver layer';
    PRINT 'ERROR Message' +ERROR_MESSAGE();
    PRINT 'ERROR Message  ' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR Message  ' + CAST (ERROR_STATE() AS NVARCHAR);
END CATCH
END
