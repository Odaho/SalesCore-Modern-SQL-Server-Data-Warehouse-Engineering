# SalesCore DW

## SQL Server Data Warehouse Engineering

### Modern Sales Data Warehouse Built with SQL Server

------------------------------------------------------------------------

## Project Overview

Organizations often store sales data across multiple operational systems
such as ERP and CRM platforms. This fragmented structure makes it
difficult to generate reliable analytical insights due to inconsistent
formats, duplicated records, and disconnected data models.

**SalesCore DW** solves this problem by designing and implementing a
centralized SQL Server data warehouse that integrates, cleanses, and
structures sales data into an analytics-ready dimensional model.

This project demonstrates end-to-end data engineering capabilities, from
ingestion to modeling.

------------------------------------------------------------------------

## Business Problem

Sales data existed in separate ERP and CRM systems provided as CSV
files.

Key challenges included:

-   Data fragmentation across systems\
-   Inconsistent identifiers and formats\
-   Duplicate and missing records\
-   No unified model optimized for analytics

Without consolidation, stakeholders cannot reliably analyze revenue
trends, customer behavior, or product performance.

------------------------------------------------------------------------

## Solution

A modern SQL Server data warehouse was designed and implemented to:

-   Import ERP and CRM data into structured staging tables\
-   Clean and standardize raw data\
-   Resolve data quality issues\
-   Integrate customer and sales entities\
-   Design a dimensional (Star Schema) data model\
-   Deliver query-optimized fact and dimension tables

The final model provides a clean, scalable foundation for BI and
analytics workloads.

------------------------------------------------------------------------

## Architecture Approach

**1. Data Ingestion**\
CSV files loaded into staging tables in SQL Server.

**2. Data Cleansing & Validation**\
- Duplicate removal\
- Null handling\
- Data type standardization\
- Key validation and reconciliation

**3. Transformation & Integration**\
- Surrogate key generation\
- Entity alignment between ERP and CRM\
- Referential integrity enforcement

**4. Dimensional Modeling**\
Designed a Star Schema consisting of:

-   FactSales\
-   DimCustomer\
-   DimProduct\
-   DimDate

The model is optimized for aggregation, filtering, and high-performance
analytical queries.

------------------------------------------------------------------------

## Data Model Design

The warehouse follows dimensional modeling best practices:

-   Fact table for measurable business events\
-   Dimension tables for descriptive attributes\
-   Surrogate keys for consistency\
-   Referential integrity enforced

This structure supports:

-   Revenue analysis\
-   Customer segmentation\
-   Product performance evaluation\
-   Time-based trend analysis

------------------------------------------------------------------------

## Tech Stack

-   Microsoft SQL Server\
-   T-SQL\
-   SQL Server Management Studio (SSMS)\
-   CSV Data Sources

------------------------------------------------------------------------

## Key Skills Demonstrated

-   Data Warehousing\
-   Dimensional Modeling (Star Schema)\
-   ETL Development (SQL-Based)\
-   Data Cleansing & Validation\
-   Schema Design & Optimization\
-   Relational Database Design\
-   Data Integration

------------------------------------------------------------------------

## Results

-   Unified ERP and CRM datasets into a centralized warehouse\
-   Eliminated data quality inconsistencies\
-   Delivered analytics-ready structured data\
-   Created scalable foundation for downstream BI reporting

------------------------------------------------------------------------

## Project Type

Data Engineering\
Data Warehousing\
SQL-Based ETL
