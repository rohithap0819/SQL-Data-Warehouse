/*
================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
================================================================
Script Purpose:
This stored procedure performs the ETL (Extract, Tranform, Load) process to populate the 'silver schema tables from the 'bronzeschema.
Actions Performed.
Truncates Silver tables.
Inserts transformed and cleansed data from Bronze into Silver tables.
Parameters:
None.
This stored procedure does not accept any parameters or return any values.
Usage Example:
EXEC Silver.load_silver;
=================================================================
*/


create or alter procedure silver.load_silver as 
begin
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		--Loading silver.crm_crust_info
		SET @start_time = GETDATE();
		print '>> Truncating table: silver.crm_crust_info';
		TRUNCATE TABLE silver.crm_crust_info;
		print '>> Inserting data Into: silver.crm_crust_info';
		insert into silver.crm_crust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gender,
			cst_create_date)

		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case when upper(trim(cst_material_status)) = 'S' then 'Single'
			 when upper(trim(cst_material_status)) = 'M' then 'Married'
			 else 'N/A' -- Normalize the material status value to readable form
		end cst_material_status,
		case when upper(trim(cst_gender)) = 'F' then 'Female'
			 when upper(trim(cst_gender)) = 'M' then 'Male'
			 else 'N/A'-- Normalize the gender value to readable form
		end cst_gender, 
		cst_create_date
		--Removing the Duplicates
		from(select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
			 from bronze.crm_crust_info
			 where cst_id is not null)t
		where flag_last = 1 -- select the most recent record per customers
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		--Loading silver.crm_prd_info
		SET @start_time = GETDATE();
		print '>> Truncating table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		print '>> Inserting data Into: silver.crm_prd_info';
		--Cleaning and Loading the Data in bronze.crm_prd_info to silver.crm_prd_info 

		insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
		SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost, 0) as prd_cost,
		case when upper(trim(prd_line)) = 'M' then 'Mountain'
			 when upper(trim(prd_line)) = 'R' then 'Road'
			 when upper(trim(prd_line)) = 'S' then 'Other Sales'
			 when upper(trim(prd_line)) = 'T' then 'Touring'
			 else 'N/A'
		END AS prd_line,
		prd_start_dt,
		DATEADD(DAY, -1, lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as prd_end_dt
		from bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		--Loading silver.crm_sales_details
		SET @start_time = GETDATE();
		print '>> Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		print '>> Inserting data Into: silver.crm_sales_details';

		insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_qunatity,
			sls_price
		)
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then NULL
			 else cast(cast(sls_order_dt as varchar) as Date )
		end as sls_order_dt,
		case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then NULL
			 else cast(cast(sls_ship_dt as varchar) as Date )
		end as sls_ship_dt,
		case when sls_due_dt = 0 or len(sls_due_dt) != 8 then NULL
			 else cast(cast(sls_due_dt as varchar) as Date )
		end as sls_due_dt,
		case when sls_sales is null or sls_sales < = 0 or sls_sales != sls_qunatity * abs(sls_price)
			 then sls_qunatity * abs(sls_price)
			 else sls_sales
		end as sls_sales,
		sls_qunatity,
		case when sls_price is null or sls_price < = 0 
			 then sls_sales / nullif(sls_qunatity, 0)
			 else sls_price
		end as sls_price
		from bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		--Loading silver.erp_cust_az12
		SET @start_time = GETDATE();
		print '>> Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		print '>> Inserting data Into: silver.erp_cust_az12';
		insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)

		select 
		case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))  -- remove 'NAS' from the data
			 else cid
		end as cid,
		case when bdate > getdate() then NULL
			 else bdate
		end as bdate,
		case when upper(trim(gen)) in ('F', 'Female') then 'Female'
			 when upper(trim(gen)) in ('M', 'Male') then 'Male'
			 else 'N/A'
		end as gen
		from bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		--Loading silver.erp_prd_a101
		SET @start_time = GETDATE();
		print '>> Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		print '>> Inserting data Into: silver.erp_loc_a101';
		insert into silver.erp_loc_a101(cid, cntry)

		select 
		replace(cid, '-', '') as cid,
		case when trim(cntry) = 'DE' then 'Germany'
			 when trim(cntry) in ('US', 'USA') then 'United States'
			 when trim(cntry) = '' or cntry is null then 'N/A'
			 else trim(cntry)
		end as cntry
		from bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		--Loading silver.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		print '>> Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		print '>> Inserting data Into: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)

		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY

	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
end
