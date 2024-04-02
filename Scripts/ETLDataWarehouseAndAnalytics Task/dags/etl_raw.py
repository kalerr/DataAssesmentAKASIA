import pyodbc
import pandas as pd

server = 'localhost'
database = 'master'
username = 'sa'
password = 'Password123'
#Connection String

connection = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';TrustConnection=yes;TrustServerCertificate=yes;')

cursor = connection.cursor()

print("connection Established sucessfully")