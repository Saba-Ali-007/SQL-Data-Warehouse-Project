-- ================================================================
-- Procedure    : bronze.load_bronze
-- Description  :
-- This stored procedure loads raw source data from CSV files
-- into the Bronze layer tables of the Data Warehouse.
--
-- The Bronze layer stores raw, unprocessed data exactly as it
-- comes from source systems (CRM and ERP).
--
-- Steps performed in this procedure:
-- 1. Truncate existing Bronze tables to remove old data
-- 2. Load fresh data from CSV files using BULK INSERT
-- 3. Track load time for each table
-- 4. Display messages for monitoring ETL progress
-- 5. Handle errors using TRY...CATCH
--
-- Source Systems:
--    CRM System
--    ERP System
--
-- Tables Loaded:
--    bronze.crm_cust_info
--    bronze.crm_prd_info
--    bronze.crm_sales_details
--    bronze.erp_loc_a101
--    bronze.erp_cust_az12
--    bronze.erp_px_cat_g1v2
--
-- Notes:
--    BULK INSERT is used for high-performance data loading.
--    TRUNCATE TABLE ensures Bronze layer always contains
--    the latest raw snapshot from source systems.
--
-- Created for learning Data Warehouse ETL pipeline.
-- ================================================================


USE DataWareHouse;
GO


-- ================================================================
-- CREATE OR ALTER PROCEDURE
-- This ensures that if the procedure already exists it will be
-- modified, otherwise it will be created.
-- ================================================================

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN


-- ================================================================
-- DECLARE VARIABLES FOR ETL TIME TRACKING
-- These variables help measure how long each table load takes.
-- ================================================================

DECLARE @StartTime DATETIME,
        @EndTime DATETIME,
        @TableStartTime DATETIME,
        @TableEndTime DATETIME;


-- Record overall ETL start time
SET @StartTime = GETDATE();


BEGIN TRY


PRINT '=====================================';
PRINT 'Starting Bronze Layer Data Load';
PRINT 'Start Time: ' + CAST(@StartTime AS VARCHAR);
PRINT '=====================================';



-- ================================================================
-- STEP 1 : LOAD CUSTOMER INFORMATION
-- Source : CRM System
-- File   : cust_info.csv
--
-- Process:
-- 1. Remove existing data using TRUNCATE
-- 2. Load fresh data using BULK INSERT
-- ================================================================

SET @TableStartTime = GETDATE();

PRINT 'Loading Customer Information Table...';

TRUNCATE TABLE bronze.crm_cust_info;

BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\HP\Desktop\PERSONAL\MCA III\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH
(
    FIRSTROW = 2,           -- Skip header row
    FIELDTERMINATOR = ',',  -- Columns separated by comma
    ROWTERMINATOR = '\n',   -- Each row ends with newline
    TABLOCK                 -- Improves bulk load performance
);

SET @TableEndTime = GETDATE();

PRINT 'Customer Table Load Time (seconds): '
      + CAST(DATEDIFF(SECOND,@TableStartTime,@TableEndTime) AS VARCHAR);



-- ================================================================
-- STEP 2 : LOAD PRODUCT INFORMATION
-- Source : CRM System
-- File   : prd_info.csv
-- ================================================================

SET @TableStartTime = GETDATE();

PRINT 'Loading Product Information Table...';

TRUNCATE TABLE bronze.crm_prd_info;

BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\HP\Desktop\PERSONAL\MCA III\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH
(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

SET @TableEndTime = GETDATE();

PRINT 'Product Table Load Time (seconds): '
      + CAST(DATEDIFF(SECOND,@TableStartTime,@TableEndTime) AS VARCHAR);



-- ================================================================
-- STEP 3 : LOAD SALES DETAILS
-- Source : CRM System
-- File   : sales_details.csv
-- ================================================================

SET @TableStartTime = GETDATE();

PRINT 'Loading Sales Details Table...';

TRUNCATE TABLE bronze.crm_sales_details;

BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\HP\Desktop\PERSONAL\MCA III\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH
(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

SET @TableEndTime = GETDATE();

PRINT 'Sales Table Load Time (seconds): '
      + CAST(DATEDIFF(SECOND,@TableStartTime,@TableEndTime) AS VARCHAR);



-- ================================================================
-- STEP 4 : LOAD ERP LOCATION DATA
-- Source : ERP System
-- File   : LOC_A101.csv
-- ================================================================

SET @TableStartTime = GETDATE();

PRINT 'Loading ERP Location Data...';

TRUNCATE TABLE bronze.erp_loc_a101;

BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\HP\Desktop\PERSONAL\MCA III\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH
(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

SET @TableEndTime = GETDATE();

PRINT 'ERP Location Load Time (seconds): '
      + CAST(DATEDIFF(SECOND,@TableStartTime,@TableEndTime) AS VARCHAR);



-- ================================================================
-- STEP 5 : LOAD ERP CUSTOMER DATA
-- Source : ERP System
-- File   : CUST_AZ12.csv
-- ================================================================

SET @TableStartTime = GETDATE();

PRINT 'Loading ERP Customer Data...';

TRUNCATE TABLE bronze.erp_cust_az12;

BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\HP\Desktop\PERSONAL\MCA III\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH
(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

SET @TableEndTime = GETDATE();

PRINT 'ERP Customer Load Time (seconds): '
      + CAST(DATEDIFF(SECOND,@TableStartTime,@TableEndTime) AS VARCHAR);



-- ================================================================
-- STEP 6 : LOAD PRODUCT CATEGORY DATA
-- Source : ERP System
-- File   : PX_CAT_G1V2.csv
-- ================================================================

SET @TableStartTime = GETDATE();

PRINT 'Loading Product Category Data...';

TRUNCATE TABLE bronze.erp_px_cat_g1v2;

BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\HP\Desktop\PERSONAL\MCA III\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH
(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

SET @TableEndTime = GETDATE();

PRINT 'Product Category Load Time (seconds): '
      + CAST(DATEDIFF(SECOND,@TableStartTime,@TableEndTime) AS VARCHAR);



-- ================================================================
-- CALCULATE TOTAL ETL LOAD TIME
-- ================================================================

SET @EndTime = GETDATE();

PRINT '=====================================';
PRINT 'Bronze Layer Load Completed';
PRINT 'Total Load Time (seconds): '
      + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR);
PRINT '=====================================';


END TRY


-- ================================================================
-- ERROR HANDLING SECTION
-- If any error occurs during the load process,
-- detailed information will be printed.
-- ================================================================

BEGIN CATCH

PRINT 'ERROR OCCURRED DURING DATA LOAD';

PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
PRINT 'Error Message: ' + ERROR_MESSAGE();
PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR);

END CATCH

END;
GO


-- ================================================================
-- EXECUTE PROCEDURE
-- This command triggers the Bronze layer data load process
-- ================================================================

EXEC bronze.load_bronze;
