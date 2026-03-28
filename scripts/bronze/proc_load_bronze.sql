/*
=====================================================
Stored procedure: Load bronze layer (Source -> bronze)
=====================================================
Script purpose:
  This stored procedure load data into the 'bronze' schema from external CSV files.
  It performs the following actions:
    -Truncates the bronze tables before loading
    -Uses the 'BULK INSERT' command to load data from csv files to bronze tables

Parameters:
  None.
This stored procedure does not accept any parameters or return any values.

Usage example:
  EXEC bronze.load_bronze;
======================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_load DATETIME, @end_load DATETIME;
    BEGIN TRY
        SET @start_load = GETDATE();
        PRINT '=======================================';
        PRINT 'Loading Bronze Layer';
        PRINT '=======================================';


        PRINT '---------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '---------------------------------------';
        
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_cust_info ';
        TRUNCATE TABLE bronze.crm_cust_info;
    
        PRINT '>> Inserting data into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\Stephane\OneDrive\Documenten\SQL course\sql - warehouse project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '+CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------------';
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_sales_details ';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting data into table: bronze.crm_sales_details ';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\Stephane\OneDrive\Documenten\SQL course\sql - warehouse project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading time: '+CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating table:bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting data into table:bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\Stephane\OneDrive\Documenten\SQL course\sql - warehouse project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading time: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------------';

        PRINT '---------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '---------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating table:bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting data into table:bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\Stephane\OneDrive\Documenten\SQL course\sql - warehouse project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading time :' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT '-------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating table:bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting data into table:bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\Stephane\OneDrive\Documenten\SQL course\sql - warehouse project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading time :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT '-------------------------------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\Stephane\OneDrive\Documenten\SQL course\sql - warehouse project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading time :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT '-------------------------------';
        SET @end_load = GETDATE();
        PRINT '=======================================================';
        PRINT 'Loading Bronze layer is completed';
        PRINT '     - Total load duration: ' + CAST(DATEDIFF(second,@start_load, @end_load) AS NVARCHAR) + ' seconds.';
        PRINT '=======================================================';
    END TRY
    BEGIN CATCH
        PRINT '=======================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message '+ ERROR_MESSAGE();
        PRINT 'Error Message '+ CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message '+ CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=======================================';
    END CATCH
END
