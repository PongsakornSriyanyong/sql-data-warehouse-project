/*Cleansing and Insert Data & Calculate time upload for each table*/
/*Use example exec silver.load_silver/*

create or alter procedure silver.load_silver as
Begin
	Declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime;
	BEGIN try
		set @batch_start_time = getdate();
		Print '================================================';
		Print 'Loading Silver Layer';
		Print '================================================';

		Print '------------------------------------------------';
		Print 'Loading CRM Tables';
		Print '------------------------------------------------';

		-- Loading silver.crm_cust_info
		set @start_time = getdate();
		Print '>> Truncating Table: silver.crm_cust_info';
		Truncate table silver.crm_cust_info;
		Print '>> Inserting data in to silver.crm_cust_info';
		insert into silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date)

		select
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname, -- Remove all space
		TRIM(cst_lastname) AS cst_lastname,
		case when Upper(TRIM(cst_material_status)) = 'S' THEN 'Single' --Check s and transfer to single
			 when Upper(TRIM(cst_material_status)) = 'M' THEN 'Married' --same with s
			 Else 'n\a' --null => n\a
		END cst_material_status,
		case when Upper(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 when Upper(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 Else 'n\a'
		END cst_gndr,
		cst_create_date
		from(
		Select 
		*,
		ROW_NUMBER() Over (partition by cst_id order by cst_create_date DESC) as flag_last
		from bronze.crm_cust_info
		where cst_id is not null)
		t where flag_last = 1
		Set @end_time = getdate();
		Print '>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) AS nvarchar) +' seconds';

		-- silver.crm_prd_info
		set @start_time = getdate();
		Print '>> Truncating Table: silver.crm_prd_info';
		Truncate table silver.crm_prd_info;
		Print '>> Inserting data in to silver.crm_prd_info';
		Insert INTO silver.crm_prd_info(
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
		REPLACE(SUBSTRING(prd_key,1,5), '-','_') as cat_id,
		SUBSTRING(prd_key,7,Len(prd_key)) As prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		case upper(trim(prd_line))
			when  'M' then 'Mountain'
			when  'R' then 'Road'
			when  'S' then 'Other Sales'
			when  'T' then 'Touring'
			else 'n\a'
		end as prd_line,
		cast(prd_start_dt as date) as prd_start_dt,
		cast(Lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) as date) as prd_end_dt_test -- subtitue
		from bronze.crm_prd_info
		Set @end_time = getdate();
		Print '>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) AS nvarchar) +' seconds';

		-- silver.crm_sales_details
		set @start_time = getdate();
		Print '>> Truncating Table: silver.crm_sales_details';
		Truncate table silver.crm_sales_details;
		Print '>> Inserting data in to silver.crm_sales_details';
		insert into silver.crm_sales_details(
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
		select
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case when sls_order_dt = 0 or len(sls_order_dt) != 8 Then Null
				 else cast(cast(sls_order_dt as varchar) as date)
			End sls_order_dt,
			case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 Then Null
				 else cast(cast(sls_ship_dt as varchar) as date)
			End sls_ship_dt,
			case when sls_due_dt = 0 or len(sls_due_dt) != 8 Then Null
				 else cast(cast(sls_due_dt as varchar) as date)
			End sls_due_dt,
			case when sls_sales != sls_quantity * abs(sls_price) 
					  or sls_sales is null
					  or sls_sales <= 0
					  then abs(sls_price) * sls_quantity
					  else sls_sales
			end sls_sales,
			sls_quantity,
			case when sls_price <= 0 OR sls_price is null
				 Then sls_sales / Nullif(sls_quantity,0)
				 else sls_price
			end sls_price
		FROM bronze.crm_sales_details
		Set @end_time = getdate();
		Print '>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) AS nvarchar) +' seconds';

		-- Insert silver.erp_cust_az12
		set @start_time = getdate();
		Print '>> Truncating Table: silver.erp_cust_az12';
		Truncate table silver.erp_cust_az12;
		Print '>> Inserting data in to silver.erp_cust_az_12';
		insert into silver.erp_cust_az12 (cid,bdate,gen)
		select
		case when cid like 'NAS%' THEN Substring(cid,4,Len(cid))
			else cid
			end cid,
		case when bdate > getdate() then null
			 else bdate
		end as bdate,
		case when upper(trim(gen)) in ('F','FEMALE') Then 'Female'
			 when upper(trim(gen)) in ('M','MALE') Then 'Male'
			 else 'n/a'
		end as gen
		from bronze.erp_cust_az12
		Set @end_time = getdate();
		Print '>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) AS nvarchar) +' seconds';

		-- Insert silver.erp_loc_a101
		set @start_time = getdate();
		Print '>> Truncating Table: silver.erp_LOC_A101 ';
		Truncate table silver.erp_LOC_A101 ;
		Print '>> Inserting data in to silver.erp_LOC_A101 ';
		insert into silver.erp_loc_a101
		(cid, cntry)
		select 
			replace(cid,'-','') as cid,
			CASE when trim(cntry) = 'DE' Then 'Germany'
				 when trim(cntry) in ('US,''USA') Then 'United States'
				 when trim(cntry) = '' or cntry is null then 'n/a'
				 else trim(cntry)
			end as cntry
		from bronze.erp_LOC_A101 
		Set @end_time = getdate();
		Print '>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) AS nvarchar) +' seconds';

		-- Insert silver.erp_px_cat_g1v2
		set @start_time = getdate();
		Print '>> Truncating Table: silver.erp_PX_CAT_G1V2 ';
		Truncate table silver.erp_PX_CAT_G1V2;
		Print '>> Inserting data in to silver.erp_PX_CAT_G1V2 ';
		insert into silver.erp_PX_CAT_G1V2(
			id,cat,subcat,maintenance
		)
		select
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_PX_CAT_G1V2
		Set @end_time = getdate();
		Print '>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) AS nvarchar) +' seconds';
		SET @batch_end_time = GETDATE();
		PRINT '==========================='
		PRINT 'Loading Silver Layer is Completed';
		PRINT ' - Total  Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) +' seconds';
	END TRY
	BEGIN CATCH
		PRINT '==========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================='
	END CATCH
END
