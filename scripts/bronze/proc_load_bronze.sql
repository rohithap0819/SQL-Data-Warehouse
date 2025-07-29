CREATE OR ALTER PROCEDURE bronze.load_bronze as
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		set @start_time = GETDATE();
		print '=====================================';
		print 'Loading BRONZE Layer';
		PRINT '=====================================';
	
		print '-------------------------------------';
		print 'Loading CRM Tables';
		PRINT '-------------------------------------';
	    SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_crust_info;

		print'>> Instering Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_crust_info
		from 'C:\Users\Rohith A P\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Time: '+ CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
		PRINT '-----------------------'


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		print'>> Instering Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		from 'C:\Users\Rohith A P\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Time: '+ CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
		PRINT '-----------------------'


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		print'>> Instering Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		from 'C:\Users\Rohith A P\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Time: '+ CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
		PRINT '-----------------------'


		print '--------------------------------------';
		print 'Loding ERP Tables';
		print '--------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		print'>> Instering Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		from 'C:\Users\Rohith A P\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Time: '+ CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
		PRINT '-----------------------'


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		print'>> Instering Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		from 'C:\Users\Rohith A P\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Time: '+ CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
		PRINT '-----------------------'


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		print'>> Instering Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		from 'C:\Users\Rohith A P\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Time: '+ CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
		PRINT '-----------------------'

		SET @end_time = GETDATE();
		PRINT '====================================';
		PRINT '>> Total Loading Time for Bronze is :' + CAST ( DATEDIFF (second, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
		PRINT '====================================';

	END TRY

	BEGIN CATCH
		PRINT '============================================';
		PRINT 'Error While Loading Bronze Layer Data';
		PRINT 'Error Number: ' + Error_Message();
		PRINT 'Error Number: ' + CAST(Error_Number() AS NVARCHAR);
		PRINT 'Error Number: ' + CAST(Error_Number() AS NVARCHAR);
		PRINT '============================================';
	END CATCH
END

EXEC bronze.load_bronze
