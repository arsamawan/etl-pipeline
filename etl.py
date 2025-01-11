# import libraries
from pyodbc import drivers
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pyodbc
import pandas as pd
# import os
pwd = 'demopass1'
uuid = 'etl'

# sql database details
driver ="{ODBC Driver 17 for SQL Server}"
# driver = "{SQL Server}"
# driver = "{SQL Server Native Client 11.0}"
server = 'localhost'
# server = ' 192.106.0.102'
# server = '192.106.0.102\SQLEXPRESS'
# haq pc server
database = 'AdventureWorksDW2019'
# print("drivers: " , pyodbc.drivers())
def extract():
    # global src_conn
    # print("Extracting...")

    try:
        connection_string = 'DRIVER=' + driver + ';SERVER=' + server + '\SQLEXPRESS' +';DATABASE=' + database + ';UID=' + uuid + ';PWD=' + pwd
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        src_engine = create_engine(connection_url)
        src_conn = src_engine.connect()
        # execute query
        query = """ select  t.name as table_name
               from sys.tables t where t.name in ('DimProduct','DimProductSubcategory','DimProductSubcategory','DimProductCategory','DimSalesTerritory','FactInternetSales') """
        src_tables = pd.read_sql_query(query, src_conn)['table_name']
        print(src_tables)

        for tbl in src_tables:
            table_name = tbl
            df = pd.read_sql_query(f'select * FROM {table_name}', src_conn)
            # print( df)
            load(df,table_name)

            # src_tables = pd.read_sql_query(query, src_conn).to_dict()['table_name']
            # for tbl in src_table:
            #     # table_name = src_tables[id]
            #     df = pd.read_sql_query(f'select * FROM {tbl[0]}', src_conn)
            #     load(df, tbl[0])


        # src_conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + '\SQLEXPRESS' +';DATABASE=' + database + ';UID=' + uuid + ';PWD=' + pwd)
        # # sql = "SELECT * FROM [dbo].[DimProductCategory]"
        # # print(sql)
        # # print(connection_string)
        # # connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        # # src_engine = create_engine(connection_url)
        # # src_conn = src_engine.connect()
        # # # print(src_conn)
        # src_cursor = src_conn.cursor()
        # # conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + '\SQLEXPRESS' +
        # #                       ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd)
        # # EXECUTE QUERY
        # src_cursor.execute(""" select  t.name as table_name from sys.tables t where t.name in ('DimProduct','DimProductSubcategory','DimProductSubcategory','DimProductCategory','DimSalesTerritory','FactInternetSales') """)
        # src_table = src_cursor.fetchall()
        # # print(src_table)
        # # connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": src_conn})
        # # src_engine = create_engine(connection_url)
        # # src_conn = src_engine.connect()
        # # # execute query
        # # query = """ select  t.name as table_name from sys.tables t where t.name in ('DimProduct','DimProductSubcategory','DimProductSubcategory','DimProductCategory','DimSalesTerritory','FactInternetSales') """
        # # src_tables = pd.read_sql_query(query, src_conn).to_dict()['table_name']
        # for tbl in src_table:
        #     # table_name = src_tables[id]
        #     df = pd.read_sql_query(f'select * FROM {tbl[0]}', src_conn)
        #     load(df, tbl[0])
    except Exception as e:
        # print(e)
        print("Data extract error: " + str(e))

    finally:
        # print('Closing connection')
        src_conn.close()

    # load data to postgres
def load(df, tbl):
    try:
        # print('entered in load')
        rows_imported = 0
        # ('postgresql+psycopg2://postgres:admin@localhost:5432/test')
            # url = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/test')
        # print(f'postgresql+psycopg2://{uuid}:demopass@{server}:5432/AdventureWorks')
        # engine = create_engine('postgresql+psycopg2://etl:your_password@localhost:your_port/AdventureWorks')

        engine = create_engine(f'postgresql://{uuid}:{pwd}@{server}:5432/AdventureWorks')
        # Test the connection
        connection = engine.connect()
        print("Connection successful!")
        connection.close()
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        # add elapsed time to final print out
        print("Data imported successful")
    except Exception as m:
        print("Data load error: " + str(m))

try:
    # print('entered in load try')
    # call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))

