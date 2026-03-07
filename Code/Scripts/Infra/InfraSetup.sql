-- CREATE INSURANCE DW database
CREATE DATABASE Insurance_DW;

USE Insurance_DW;

--CREATE SCHEMAS
CREATE SCHEMA BRONZE;
GO
CREATE SCHEMA SILVER;
GO
CREATE SCHEMA GOLD;
GO
CREATE SCHEMA METADATA;
GO

-- CREATE ROLES
CREATE ROLE data_engineer;
CREATE ROLE data_analyst;
CREATE ROLE dw_admin;

-- GRANTING PRIVILEGES

-- DATA ENGINEERS
GRANT SELECT, INSERT,UPDATE, DELETE ON SCHEMA::bronze TO data_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::silver TO data_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::gold TO data_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::metadata TO data_engineer;

-- DATA ANALYST
GRANT SELECT ON SCHEMA::gold TO data_analyst;

-- ADMIN
GRANT CONTROL ON DATABASE::Insurance_DW TO dw_admin;


---- LETS US CREATES SOME EMPLOYEES

-- SHREEYA
--Full control
--Create tables
--Modify schema
--Manage users
--Run pipelines

-- RAHUL
--Insert data into bronze
--Transform data into silver
--Build gold tables
--Run ETL pipelines

--PRACHI
--SELECT from gold tables
--Create reports
--Query warehouse

-- SERVER LEVEL ACCESS
CREATE LOGIN Shreeya2001 WITH PASSWORD = 'Anj@ni2001';
CREATE LOGIN Rahul2025 WITH PASSWORD = 'Ollemode@2025!';
CREATE LOGIN Prachi04 WITH PASSWORD = 'Argon!2081';

-- DATABASE LEVEL ACCESS
USE Insurance_DW;
GO
CREATE USER Shreeya FOR LOGIN Shreeya2001;
CREATE USER Rahul FOR LOGIN Rahul2025;
CREATE USER Prachi FOR LOGIN Prachi04;

-- ASSIGNING ROLES TO USERS
ALTER ROLE dw_admin ADD MEMBER Shreeya;
ALTER ROLE data_analyst ADD MEMBER Prachi;
ALTER ROLE data_engineer ADD MEMBER Rahul;

-- script to check whether the process worked
SELECT name,type_desc
FROM sys.database_principals
WHERE type IN ('S','U','R');