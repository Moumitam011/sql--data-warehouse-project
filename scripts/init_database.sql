/*
==============================================================
Create Database and Schemas
==============================================================

Script Purpose:
    This script creates a new database named 'DataWareHouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.
*/
USE master;
GO

-- Check if database exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- Force disconnect users and rollback any running transactions
    ALTER DATABASE DataWarehouse 
    SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    -- Drop the database
    DROP DATABASE DataWarehouse;
END;
GO

-- Create fresh database
CREATE DATABASE DataWarehouse;
GO

-- Switch to new database
USE DataWarehouse;
GO

-- Create schemas (Medallion Architecture)
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
