import pyodbc as odbc

server='HP-SERVER\\MSSQLSERVER02'
port = 3050
username='DCTDeveloper'
pwd =  'developer@123'
database  ='primary'
conn_obj = odbc.connect('driver={%s};server=%s;port=%s;uid=%s;pwd=%s;database=%s;Trusted_Connection=yes' % ('SQL Server',server, port ,username ,pwd, database ))
cusrcor = conn_obj.cursor()
cusrcor.execute('SELECT TOP (1000) [customer_id] ,[customer_unique_id] ,[customer_zip_code_prefix],[customer_city],[customer_state] FROM [primary].[dbo].[olist_customers_dataset]')
print(cusrcor.fetchall()) 

