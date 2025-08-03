CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '================================';
	
		PRINT '--------------------------------';
		PRINT 'Loading CRM Data';
		PRINT '--------------------------------';
		TRUNCATE TABLE bronze.crm_cust_info;
		
		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Time taken to load bronze.crm_cust_info: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------';

		TRUNCATE TABLE bronze.crm_prd_info;
		
		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Time taken to load bronze.crm_prd_info: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------';


		TRUNCATE TABLE bronze.crm_sales_details;
		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Time taken to load bronze.crm_sales_details: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------';

		PRINT '--------------------------------';
		PRINT 'Loading ERP Data';
		PRINT '--------------------------------';

		TRUNCATE TABLE bronze.erp_cust_az12;
		SET @start_time = GETDATE();
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\SQL\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK	
		);
		SET @end_time = GETDATE();
		PRINT 'Time taken to load bronze.erp_cust_az12: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------';

		TRUNCATE TABLE bronze.erp_loc_a101;
		SET @start_time = GETDATE();
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\SQL\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK	
		);
		SET @end_time = GETDATE();
		PRINT 'Time taken to load bronze.erp_loc_a101: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------';

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		SET @start_time = GETDATE();
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\SQL\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK	
		);
		SET @end_time = GETDATE();
		PRINT 'Time taken to load bronze.erp_loc_a101: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------';
	END TRY
	BEGIN CATCH
		PRINT '=========================================';
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '=========================================';
	END CATCH
	SET @batch_end_time = GETDATE();
	PRINT'Time taken to load batch in bronze layer: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
END
