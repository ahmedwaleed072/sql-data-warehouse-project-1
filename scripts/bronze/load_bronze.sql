/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

create or alter procedure bronze.load_bronze as
begin
	declare @beginning_time datetime, @ending_time datetime, @batch_beginning_time datetime, @batch_ending_time datetime; 
    begin try
		set @batch_beginning_time = getdate();
		print '==============================================================';
		print 'Loading the Bronze Layer';
		print '==============================================================';

		print '--------------------------------------------------------------';
		print 'Loading the CRM Tables';
		print '--------------------------------------------------------------';
		set @beginning_time = getdate();
		print '>> Truncating the table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		print '>> Inserting the Data into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'D:\Ventures\SQL Learning\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @ending_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @beginning_time, @ending_time) as nvarchar) +'  Second(s)';
		print '>>---------------';

		set @beginning_time = getdate();
		print '>> Truncating the table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		print '>> Inserting the Data into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'D:\Ventures\SQL Learning\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @ending_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @beginning_time, @ending_time) as nvarchar) +'  Second(s)';
		print '>>---------------';

	    set @beginning_time = getdate();
		print '>> Truncating the table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		print '>> Inserting the Data into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'D:\Ventures\SQL Learning\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @ending_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @beginning_time, @ending_time) as nvarchar) +'  Second(s)';
		print '>>---------------';

		print '--------------------------------------------------------------';
		print 'Loading the ERP Tables';
		print '--------------------------------------------------------------';

		set @beginning_time = getdate();
		print '>> Truncating the table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		print '>> Inserting the Data into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'D:\Ventures\SQL Learning\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @ending_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @beginning_time, @ending_time) as nvarchar) +'  Second(s)';
		print '>>---------------';

		set @beginning_time = getdate();
		print '>> Truncating the table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		print '>> Inserting the Data into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'D:\Ventures\SQL Learning\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @ending_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @beginning_time, @ending_time) as nvarchar) +'  Second(s)';
		print '>>---------------';


		set @beginning_time = getdate();
		print '>> Truncating the table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		print '>> Inserting the Data into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\Ventures\SQL Learning\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @ending_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @beginning_time, @ending_time) as nvarchar) +'  Second(s)';
		print '>>---------------';
		set @batch_ending_time = getdate();
		print '=============================================================='
		print 'Bronze Layer Load is Completed';
		print '  -Total Load Duration: ' +  cast(datediff(second, @batch_beginning_time, @batch_ending_time) as nvarchar) +'  Second(s)';
		print '=============================================================='
	end try
	begin catch
	print '=============================================================='
    print 'Error Occured During Loading The Bronze Layer'
	print 'Error Message' + Error_Message();
	print 'Error Message' + Cast (Error_Number() as nvarchar);
	print 'Error Message' + Cast (Error_state() as nvarchar);
    print '==============================================================' 
	end catch 
	print 'Found No Errors'
end;
