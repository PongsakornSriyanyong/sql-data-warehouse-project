/* Usage EXEC bronze.load_bronze;
  to import data from csv files and check load times
  truncate tables before loading data
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS --create or adjust table
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_strat_time DATETIME, @batch_end_time DATETIME; --Batch_start and batch_end = Overall table, start_time and end_time = Each table
	BEGIN TRY
		SET @batch_strat_time = GETDATE();
		PRINT '==========================='
		PRINT 'Loading Bronze Layer'
		PRINT '==========================='

		PRINT '---------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '---------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info; -- delete data for protect duplicate data before insert new data
		PRINT '>> Inserting Data Into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info -- Import data into table
		FROM 'C:\Users\lenovo\AppData\Local\Programs\Sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- First row is header so start read second row
			FIELDTERMINATOR = ',', -- define ตัวคั่นข้อมูล
			TABLOCK
		);
		SET @end_time = GETDATE(); -- define time after finish process
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; -- Start_time - end_time = Process_time
		PRINT '>> --------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> Inserting Data Into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\lenovo\AppData\Local\Programs\Sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> Inserting Data Into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\lenovo\AppData\Local\Programs\Sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------'

		PRINT '---------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '---------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_CUST_AZ12'
		TRUNCATE TABLE bronze.erp_CUST_AZ12
		PRINT '>> Inserting Data Into: bronze.erp_CUST_AZ12'
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\lenovo\AppData\Local\Programs\Sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_LOC_A101'
		TRUNCATE TABLE bronze.erp_LOC_A101
		PRINT '>> Inserting Data Into: bronze.erp_LOC_A101'
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\lenovo\AppData\Local\Programs\Sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_PX_CAT_G1V2'
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2
		PRINT '>> Inserting Data Into: bronze.erp_PX_CAT_G1V2'
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\lenovo\AppData\Local\Programs\Sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------'

		SET @batch_end_time = GETDATE();
		PRINT '==========================='
		PRINT 'Loading Bronze Layer is Completed';
		PRINT ' - Total  Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) +' seconds';
	END TRY
	BEGIN CATCH
		PRINT '==========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR)
		PRINT 'Error Message' + CAST (ERROR_STATE() AS 
		PRINT '==========================================='
	END CATCH
END
