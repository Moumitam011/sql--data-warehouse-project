/*
====================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
====================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

--FULLLOAD : FIRST TRUNCATE THE WHOLE TABLE(EMPTY) THEN AGAIN LOAD FROM SCRATCH
-- CREATE STOREPROCEDURE AS LOADING IS DONE EVERYDAY
====================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze as
BEGIN
  DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	    SET @batch_start_time = GETDATE();
		PRINT ('=======================================================')
		PRINT ('LOADING  BRONZE LAYER')
		PRINT ('=======================================================')


		PRINT ('-------------------------------------------------------');
		PRINT ('loading CRM tables');
		PRINT ('-------------------------------------------------------');		

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table :  bronze.crm_cust_info';
		TRUNCATE TABLE  bronze.crm_cust_info;

		PRINT '>> Inserting Data into Table :  bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		from 'C:\Users\MOUMITA\Documents\SQL Server Management Studio 21\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK

		);

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST (DATEDIFF(Second, @start_time, @end_time) AS NVARCHAR) + 'Seconds' 
		PRINT '---------------------' 

		SET @start_time = GETDATE();
		
		PRINT '>> Truncating Table :  bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data into Table :  bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info 
		from 'C:\Users\MOUMITA\Documents\SQL Server Management Studio 21\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
				FIRSTROW =2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		
		SET @end_time = GETDATE();
		PRINT '>> Load duration: : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '---------------------' 

		SET @start_time = GETDATE();
		
		PRINT '>> Truncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data into Table :  bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details 
		from 'C:\Users\MOUMITA\Documents\SQL Server Management Studio 21\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '---------------------' 



		-- LOAD ERP

		PRINT ('-------------------------------------------------------');
		PRINT ('loading ERP tables');
		PRINT ('-------------------------------------------------------');
	
	    SET @start_time = GETDATE();

		PRINT '>> Truncating Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data into Table :  bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		from 'C:\Users\MOUMITA\Documents\SQL Server Management Studio 21\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '---------------------' 


		SET @start_time = GETDATE();

		PRINT '>> Truncating Table :bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data into Table :  bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		from 'C:\Users\MOUMITA\Documents\SQL Server Management Studio 21\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '---------------------' 

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table :bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data into Table :  bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		from 'C:\Users\MOUMITA\Documents\SQL Server Management Studio 21\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '---------------------' 

		SET @batch_end_time = GETDATE();
		PRINT '==============================' 
		Print '>> Batch loading duration : ' + CAST( DATEDIFF(Second, @batch_start_time, @batch_end_time) as NVARCHAR) + 'Second'
		PRINT '==============================' 

 END TRY
 BEGIN CATCH
		PRINT 'ERROR occured during loading the BRONZE layer';
		print 'ERROR Message  ' + ERROR_MESSAGE();
		print 'ERROR Message  ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		print 'ERROR Message  ' + CAST (ERROR_STATE() AS NVARCHAR);
 END CATCH
END

