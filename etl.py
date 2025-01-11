# import libraries
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
# import os
pwd = 'demopass1'
uuid = 'etl'

# sql database details
driver ="{ODBC Driver 17 for SQL Server}"
server = 'localhost'
database = 'AdventureWorksDW2019'
def extract():
    try:
        connection_string = 'DRIVER=' + driver + ';SERVER=' + server + '\SQLEXPRESS' +';DATABASE=' + database + ';UID=' + uuid + ';PWD=' + pwd
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        src_engine = create_engine(connection_url)
        src_conn = src_engine.connect()
        # execute query
        query = """ select  t.name as table_name
               from sys.tables t where t.name in ('DimProduct',
               'DimProductSubcategory','DimProductSubcategory',
               'DimProductCategory','DimSalesTerritory','FactInternetSales') """
        src_tables = pd.read_sql_query(query, src_conn)['table_name']

        for tbl in src_tables:
            table_name = tbl
            df = pd.read_sql_query(f'select * FROM {table_name}', src_conn)
            load(df,table_name)

    except Exception as e:
        print("Data extract error: " + str(e))

    finally:
        src_conn.close()

    # load data to postgres
def load(df, tbl):
    try:
        rows_imported = 0

        engine = create_engine(f'postgresql://{uuid}:{pwd}@{server}:5432/AdventureWorks')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        # add elapsed time to final print out
        print("Data imported successful")
    except Exception as m:
        print("Data load error: " + str(m))

try:
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))

