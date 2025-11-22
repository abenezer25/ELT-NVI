# MSSQL → PostgreSQL ETL with Metrics (In Go!)

During my internship, I needed to connect a BI tool (Metabase) to the sales data for the Sales Manager. The Database Admin vetoed direct access to the production MSSQL server due to security concerns.  

## Solution

Build an isolated data sandbox. This custom Go pipeline:  

1. **Extracts** only the required sales data from MSSQL.  
2. **Transforms & Loads** the data into a dedicated PostgreSQL replica (`SalesDB`).  
3. **Tracks ETL metrics** (row counts, errors, execution time) in MongoDB for monitoring and auditing.  

## Key Features

- **Sales Insights:**  
  Sales Managers can access actionable sales data in Metabase, querying only the safe PostgreSQL replica.  

- **ETL Performance Monitoring:**  
  DBA and SA can track ETL metrics directly in Metabase via MongoDB, allowing them to monitor pipeline health and performance.  





