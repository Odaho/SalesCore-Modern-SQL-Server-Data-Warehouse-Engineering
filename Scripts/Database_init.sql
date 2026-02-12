/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'SalesCore_DW' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'SalesCore_DW' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

--Drop and create the SalesCore_DW database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'SalesCore_DW')
BEGIN
	ALTER DATABASE SalesCore_DW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE SalesCore_DW;

END;
GO


-- Creating the main warehouse databse

CREATE DATABASE SalesCore_DW;
GO

USE SalesCore_DW;
GO

---Creating different Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
