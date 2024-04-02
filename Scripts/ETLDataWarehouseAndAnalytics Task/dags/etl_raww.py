import pyodbc
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import logging
import os
import datetime

# Do logging to track our etl done in cron job
dir_path = os.path.dirname(os.path.realpath(__file__))
filename = os.path.join(dir_path, 'etl_log.txt')

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(filename)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

def do_logging(timestamp,description):
    logger.info(f'{description} done at {timestamp}')


# if __name__ == '__main__':
#     do_logging()

# Define a function to connect to SQL Server and retrieve data into a DataFrame
def get_employee_from_sql_server():
    # Define the connection string
    server = 'localhost'
    database = 'master'
    username = 'sa'
    password = 'Password123'
    query = 'SELECT * FROM master.dbo.Employee'
    conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustConnection=yes;TrustServerCertificate=yes;'
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df

def load_employee_to_gbq(df,SA_PATH):
    credentials = service_account.Credentials.from_service_account_file(SA_path)
    project_id = "data-test-4"
    dateset_ref = "dwh"
    df = df.astype(str)
    dfToGBQ = df.to_gbq(destination_table=f'{dateset_ref}.DimEmployee',  project_id=project_id, credentials=credentials, if_exists="replace")
    return dfToGBQ

def extract_position_history_from_gbq(SA_PATH):
    SCOPES = [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/cloud-platform'
    ]
    credentials = service_account.Credentials.from_service_account_file(
        SA_PATH,
        scopes=SCOPES
    )
    project_id = "data-test-4"
    dateset_ref = "dwh"
    # client = bigquery.Client.from_service_account_json(SA_PATH)
    client = bigquery.Client(credentials=credentials, project=project_id)
    sql = f"""
        SELECT *
        FROM `{project_id}.{dateset_ref}.DimPositionHistory`
    """

    query_job = client.query(sql)
    df = query_job.to_dataframe()
    return df

SA_path = '/Users/kalerbramastha/Library/Mobile Documents/com~apple~CloudDocs/bram/career_assessment/akasia_de/DataAssesmentAKASIA/Scripts/ETLDataWarehouseAndAnalytics Task/SA.json'

# try:
# Exract Employee data from SQL Server, Load it to BigQuery as a dim
dfEmployee = get_employee_from_sql_server()

loadEmployee = load_employee_to_gbq(dfEmployee,SA_path)
do_logging(datetime.datetime.now(),"Extracting Employee from SQL Server")

#Extract DimPositionHistory 
dfPositionHistory = extract_position_history_from_gbq(SA_path)

#Transform DimEmployee and DimPositionHistory to create a TrainingHistory fact table

# Convert to datetime
dfPositionHistory['StartDate'] = pd.to_datetime(dfPositionHistory['StartDate'])
dfPositionHistory['EndDate'] = pd.to_datetime(dfPositionHistory['EndDate'])

# Calculate the duration in days
dfPositionHistory['training_day_duration'] = (dfPositionHistory['EndDate'] - dfPositionHistory['StartDate']).dt.days

# Create a new column 'rn' which is the row number partitioned by 'EmployeeId' and ordered by 'StartDate'
dfPositionHistory['rn'] = dfPositionHistory.sort_values('StartDate').groupby('EmployeeId').cumcount() + 1

# Convert 'employeeId' in 'positionhistory' from int to str
dfPositionHistory['EmployeeId'] = dfPositionHistory['EmployeeId'].astype(str)

# Perform a left join on 'employee' and 'positionhistory' using 'EmployeeId'
merged_df = pd.merge(dfEmployee, dfPositionHistory, on='EmployeeId', how='left')

# Select the rows where 'rn' is 1
result_df = merged_df[merged_df['rn'] == 1]

# Select only 'EmployeeId' and 'training_day_duration' columns
final_df = result_df[['EmployeeId', 'StartDate','EndDate','training_day_duration']]

print(final_df)

    
# except Exception as e:
#     print("Data load error: " + str(e))
