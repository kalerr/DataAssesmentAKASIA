import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
# from airflow.hooks.base_hook import BaseHook
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import pyodbc

# Define a function to connect to SQL Server and retrieve data into a DataFrame
def get_data_from_sql_server(server, database, username, password, query):
    # Define the connection string
    conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustConnection=yes;TrustServerCertificate=yes;'
    
    # Establish a connection to the database
    conn = pyodbc.connect(conn_str)
    
    # Read data into a DataFrame
    df = pd.read_sql(query, conn)
    
    # Close the connection
    conn.close()
    
    # Return the DataFrame
    return df

#extract tasks
@task()
def sql_server_extract():
    try:
        # hook = MsSqlHook(mssql_conn_id="mssql_default")
        server = 'localhost'
        database = 'master'
        username = 'sa'
        password = 'Password123'
        query = 'SELECT * FROM master.dbo.Employee'
        # df = hook.get_pandas_df(sql)
        df = get_data_from_sql_server(server, database, username, password, query)
        # print(df.head())
        print(df)
        # tbl_dict = df.to_dict('dict')
        return df
    except Exception as e:
        print("Data extract error: " + str(e))

@task()
def gcp_load_employee(df):
    #
    try:
        credentials = service_account.Credentials.from_service_account_file( '/Users/kalerbramastha/Library/Mobile Documents/com~apple~CloudDocs/bram/career_assessment/akasia_de/DataAssesmentAKASIA/Scripts/ETLDataWarehouseAndAnalytics Task/SA.json')
        project_id = "data-test-4"
        dateset_ref = "dwh"
        df = df.astype(str)
        df.to_gbq(destination_table=f'{dateset_ref}.DimEmployee',  project_id=project_id, credentials=credentials, if_exists="replace")

        # 
        # for value in employee_dict.values():
        #     #print(value)
        #     val = value.values()
        #     for v in val:
        #         #print(v)
        #         rows_imported = 0
        #         sql = f'select * FROM {v}'
        #         hook = MsSqlHook(mssql_conn_id="sqlserver")
        #         df = hook.get_pandas_df(sql)
        #         print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {v} ')
                # df.to_gbq( destination_table=f'{dateset_ref}.Dim{v}',  project_id=project_id, credentials=credentials, if_exists="replace" )
                # rows_imported += len(df)
        # 
        # for value in position_history_dict.values():
        #     #print(value)
        #     val = value.values()
        #     for v in val:
        #         #print(v)
        #         rows_imported = 0
        #         sql = f'select * FROM {v}'
        #         hook = MsSqlHook(mssql_conn_id="sqlserver")
        #         df = hook.get_pandas_df(sql)
        #         print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {v} ')
        #         df.to_gbq( destination_table=f'{dateset_ref}.Dim{v}',  project_id=project_id, credentials=credentials, if_exists="replace" )
        #         rows_imported += len(df)
        
    except Exception as e:
        print("Data load error: " + str(e))

@task()
def gcp_extract_PositionHistory():
    #
    try:
        credentials = service_account.Credentials.from_service_account_file( '/Users/kalerbramastha/Library/CloudStorage/OneDrive-Personal/career_assessment/akasia_de/Scripts/ETL, Data Warehouse and Analytics Task/service_account.json')
        project_id = "data-test-4"
        dateset_ref = "raw"
        client = bigquery.Client()

        sql = f"""
            SELECT *
            FROM `{project_id}.{dateset_ref}.RawPositionHistory`
        """

        query_job = client.query(sql)
        df = query_job.to_dataframe()
        tbl_dict = df.to_dict('dict')
        return tbl_dict
    except Exception as e:
        print("Data load error: " + str(e))

# [START how_to_task_group]
with DAG(dag_id="product_etl_dag",schedule_interval="0 9 * * *", start_date=datetime(2024, 4, 1),catchup=False,  tags=["product_model"]) as dag:

    with TaskGroup("extract_RawEmployee_load", tooltip="Extract and load source data") as extract_load_src:
        exract_src_employee_tbl = sql_server_extract()
        load_employee_tbl = gcp_load_employee(exract_src_employee_tbl)
        #define order
        exract_src_employee_tbl >> load_employee_tbl

    # with TaskGroup("extract_RawPositionHistory_load_dwh", tooltip="Transform and load data to dwh") as transform_load_src_product:
    #     transform_srcProduct = gcp_extract_PositionHistory()
    #     exract_src_product_tbls = sql_server_extract()
    #     transform_srcProductCategory = transform_srcProductCategory()
    #     #define task order
    #     [transform_srcProduct, transform_srcProductSubcategory, transform_srcProductCategory]

    # with TaskGroup("load_product_model", tooltip="Final Product model") as load_product_model:
    #     prd_Product_model = prdProduct_model()
    #     #define order
    #     prd_Product_model

    extract_load_src
    # >> transform_src_product >> load_product_model
