# SQL Server to PostgreSQL ETL Script

This script extracts data from specific tables in a Microsoft SQL Server database, transforms it as necessary, and loads it into a PostgreSQL database. It automates the ETL (Extract, Transform, Load) process for specified tables.

## Features

1. **Extract**: Fetches data from predefined tables in the SQL Server database.
2. **Load**: Transfers the extracted data into a PostgreSQL database, with each table prefixed by `stg_` (staging).
3. **Logging**: Provides progress and error messages during the extraction and loading processes.

---

## Prerequisites

- **Python 3.8+**
- **Databases**:
  - A Microsoft SQL Server database (e.g., `AdventureWorksDW2019`).
  - A PostgreSQL database to store the transformed data.
- **Dependencies**:
  Install the required Python libraries:
  ```bash
  pip install sqlalchemy pandas pyodbc psycopg2-binary
  ```

---

## Configuration

### 1. Database Connection Details: (local)
- **SQL Server**: 
  - Driver: `{ODBC Driver 17 for SQL Server}`
  - Server: `localhost\SQLEXPRESS`
  - Database: `AdventureWorksDW2019`
  - User: `etl`
  - Password: `demopass1`

- **PostgreSQL**:
  - Server: `localhost`
  - Port: `5432`
  - Database: `AdventureWorks`
  - User: `etl`
  - Password: `demopass1`


### 2. Tables to Extract:
The script extracts data from these predefined SQL Server tables:
- `DimProduct`
- `DimProductSubcategory`
- `DimProductCategory`
- `DimSalesTerritory`
- `FactInternetSales`

---

## How It Works

1. **Extract**:
   - Establishes a connection to the SQL Server database.
   - Queries the list of predefined tables.
   - Fetches all rows from each table into a Pandas DataFrame.

2. **Load**:
   - Connects to the PostgreSQL database.
   - Writes the extracted data into PostgreSQL

3. **Error Handling**:
   - Catches and logs any exceptions during extraction or loading.

---

## Running the Script
   Execute the script in your terminal:
   ```bash
   python etl_script.py
   ```

---
## Notes

- **Data Overwrite**:
  - The script uses `if_exists='replace'` when writing to PostgreSQL. This overwrites existing data in the target table.
  - Adjust to `if_exists='append'` to append new data.

- **Database User Permissions**:
  Ensure the SQL Server and PostgreSQL users have the necessary **read/write permissions.**
