from sqlalchemy import null
from db_connection import conn
from datetime import datetime
from time import sleep
from controller.Data.make_db_connection import sqlserver_con, mysql_con
import pandas as pd
import pyodbc as odbc
from threading import Timer


def main():
    runable_process = get_runnable_process()
    for process in runable_process.itertuples():
        try:
            process_master_id = process.PROCESS_MASTER_ID
            update_process_master(process_master_id,"RUNNING")
            target, source_tables, target_table, host, port, user, password, database = fetch_process_details(process_master_id)
            # print(target, source_tables, target_table, host, port, user, password, database)
            con_obj = get_connection_obj(target,host, port, user, password,database)
            # print(con_obj)
            if con_obj['status']:
                exection_process(target,host, port, user, password,database,process_master_id)
                status = 'SUCCESS'
        except Exception as e:
            status = 'FAILED'
            print(e)
        finally:
            update_process_status(process_master_id, status)
            # print('finally')

def exection_process(target,host, port, user, password,database,process_master_id):
    process_queries = get_process_queries(process_master_id)
    # print(process_queries)
    odbc_connection = get_odbc_connection(target,host, port, user, password,database)
    # print(odbc_connection)
    cursor = odbc_connection.cursor()
    cursor.execute("IF NOT EXISTS ( SELECT  * FROM  sys.schemas WHERE   name = N'TMP' ) EXEC('CREATE SCHEMA [TMP]')")
    query_report_dictionary = {} 
    generatetable = []
    index = 1
    for query in process_queries.itertuples():
        try:
            if query.QUERY_TYPE == 'select':
                lowercase_query = query.QUERY.lower()
                # print(query.DEPENDENT_QUERY)
                if query.DEPENDENT_QUERY=="":
                    select_query_run=lowercase_query.replace("from", " into [TMP].[process"+str(index)+str(process_master_id)+"] from ")
                    # print('independent',select_query_run)
                    query_report_dictionary['process'+str(index)] = cursor.execute(select_query_run)
                    generatetable.append("[TMP].[process"+str(index)+str(process_master_id)+"]")
                else:
                    temp_query=lowercase_query.replace(query.DEPENDENT_QUERY,"[TMP].["+query.DEPENDENT_QUERY+str(process_master_id)+"]") 
                    select_dependent_query_run=temp_query.replace("from", " into [TMP].[process"+str(index)+str(process_master_id)+"] from ")
                    query_report_dictionary['process'+str(index)] = cursor.execute(select_dependent_query_run)
                    # print('depedent',select_dependent_query_run)
                    generatetable.append("[TMP].[process"+str(index)+str(process_master_id)+"]")
            elif query.QUERY_TYPE == 'insert':
                if query.DEPENDENT_QUERY == "":
                    # print(query.QUERY)
                    query_report_dictionary['process'+str(index)] = cursor.execute(query.QUERY)
                else:
                    lowercase_query = query.QUERY.lower()
                    insert_query_run=lowercase_query.replace("from "+query.DEPENDENT_QUERY,"select * from [TMP].["+query.DEPENDENT_QUERY+str(process_master_id)+"]")
                    # print(insert_query_run)
                    query_report_dictionary['process'+str(index)] = cursor.execute(insert_query_run)
            elif query.QUERY_TYPE == 'alter':
                if query.DEPENDENT_QUERY == "":
                    # print(query.QUERY)
                    query_report_dictionary['process'+str(index)] = cursor.execute(query.QUERY)
                else:
                    dependent_alter_query=query.QUERY.replace(query.DEPENDENT_QUERY,query.DEPENDENT_QUERY+str(process_master_id))
                    query_report_dictionary['process'+str(index)] = cursor.execute(dependent_alter_query)
                    # print(dependent_alter_query)
                    
            else:
                if query.DEPENDENT_QUERY =="":
                    query_report_dictionary['process'+str(index)] = cursor.execute(query.QUERY)
                    # print(query.QUERY)
                else:
                    depedent_create_query=query.QUERY.replace(query.DEPENDENT_QUERY,"[TMP].["+query.DEPENDENT_QUERY+str(process_master_id)+"]")
                    query_report_dictionary['process'+str(index)] = cursor.execute(depedent_create_query)
                    # print(depedent_create_query)
            update_process_tranction(query.PROCESS_TRANSACTION_ID,process_master_id,"SUCCESS")
            
        except Exception as e:
            print(str(e))
            execution_end = datetime.now()
            log_data = {
                'process_master_id': process_master_id,
                'subprocess_id' : query.PROCESS_TRANSACTION_ID,
                'execution_report_id' : query.EXECUTION_REPORT_ID,
                'execution_status': 'Failed',
                'execution_start': execution_end,
                'execution_end': execution_end,
                'records_effected': 0,
                'error_message': str(e)
            }
            logging_process_history(log_data)
            update_process_tranction(query.PROCESS_TRANSACTION_ID,process_master_id,"FAILED")
            if(len(generatetable)>0):
                for table in generatetable:
                    query = "DROP TABLE " + table
                    cursor.execute(query)
                # print(generatetable)
            generatetable=[]
            raise Exception()
        index += 1
    if(len(generatetable)>0):
        for table in generatetable:
            query = "DROP TABLE " + table
            cursor.execute(query)
    # print(generatetable)
    cursor.commit()
    generatetable=[]
    
# def allTempTableDrop(tablename,target,host, port, user, password,database):
#     odbc_connection = get_odbc_connection(target,host, port, user, password,database)
#     cursor = odbc_connection.cursor()
#     query = "DROP TABLE " + tablename
#     print(query)
#     data1234 = cursor.execute(query)
#     print(data)

def get_runnable_process():
    runable_processes = pd.read_sql("EXEC GET_RUNABLE_PROCESSES", conn)
    return runable_processes
def update_process_tranction(process_tranction_id,process_master_id,status):
    try:
        connection = conn.raw_connection()
        cursor =connection.cursor()
        cursor.execute(f"EXEC UPDATE_PROCESS_TRANCTION_STATUS {process_tranction_id}, {process_master_id},'{status}'")
    except Exception as e:
        print(str(e))
        raise Exception("Failed to change status.")
    finally:
        cursor.close()
        connection.commit()
        connection.close()

def update_process_master(process_id,status):
    try:
        connection = conn.raw_connection()
        cursor =connection.cursor()
        cursor.execute(f"EXEC UPDATE_PROCESS_STATUS {process_id},'{status}'")
    except Exception as e:
        print(str(e))
        raise Exception("Failed to change status.")
    finally:
        cursor.close()
        connection.commit()
        connection.close()
def fetch_process_details(master_id):
    try:
        process_details = pd.read_sql(f"EXEC [GET_PROCESS_DETAILS] {master_id}", con = conn)
        target = process_details.iloc[0].TARGET
        source_tables = process_details.iloc[0].SOURSE_TABLE.split(',')
        target_table = process_details.iloc[0].TERGATE_NAME
        host = process_details.iloc[0].HOST
        port = process_details.iloc[0].PORT
        user = process_details.iloc[0].USER
        password = process_details.iloc[0].PASSWORD
        database = process_details.iloc[0].DATABASE
        return target, source_tables, target_table, host, port, user, password, database
    except Exception as e:
        raise Exception("Failed to fetch processor details.")
def get_connection_obj(target,host, port, user, password, database):
    # print(host, port, user, password, database)
    conn_obj = {}
    if target.lower() == 'sqlserver':
        conn_obj = sqlserver_con(host, port, user, password, database)
    elif target.lower() == 'mysql':
        conn_obj = mysql_con(host, port, user, password, database)
    return conn_obj
def get_odbc_connection(target,server, port, username, pwd,database):
    if target.lower() == 'sqlserver':
        conn_obj = odbc.connect('driver={%s};server=%s;port=%s;uid=%s;pwd=%s;database=%s;Trusted_Connection=yes' % ('SQL Server',server, port, username, pwd, database))
        # conn_obj = odbc.connect('driver={%s};server=%s;port=%s;uid=%s;pwd=%s;database=%s;Trusted_Connection=yes' % ('SQL Server',server, port ,username ,pwd, database ))
        # conn_obj = odbc.connect('driver={%s};server=%s;port=%s;uid=%s;pwd=%s;database=%s;Trusted_Connection=yes' % ('SQL Server',host, port, user,password,database ))
    elif target.lower() == 'mysql':
        conn_obj = mysql_con(server, port, username, pwd,database)
    return conn_obj
def get_process_queries(master_id):
    queries = pd.read_sql(f'EXEC [GET_PROCESS_QUERIES] {master_id}', conn)
    return queries
def logging_process_history(data):
    try:
        data['error_message'] = data['error_message'].replace ("'","''").replace("\\","")
        data['execution_start'] = datetime.strftime(data['execution_start'], '%Y-%m-%d %H:%M:%S')
        if data['execution_end'] is not null:
            data['execution_end'] = datetime.strftime(data['execution_end'], '%Y-%m-%d %H:%M:%S') 
        connection = conn.raw_connection()
        cursor = connection.cursor()
        cursor.execute(f"EXEC LOG_EXECUTION_REPORT {data['process_master_id']},{data['subprocess_id']},{data['execution_report_id']},'{data['execution_status']}','{data['execution_start']}','{data['execution_end']}',{data['records_effected']},'{data['error_message']}'")
    except Exception as e:
        print(str(e))
    finally:
        cursor.close()
        connection.commit()
        connection.close()
def update_process_status(process_master_id, status):
    try:
        connection = conn.raw_connection()
        cursor = connection.cursor()
        cursor.execute(f"EXEC UPDATE_PROCESS_STATUS {process_master_id}, '{status}'")
    except Exception as e:
        print(str(e))
        raise Exception("Failed to change status.")
    finally:
        cursor.close()
        connection.commit()
        connection.close()

while True:
    try:
        main()
        # sleep(1)
    except Exception as e:
        # sleep(1)
        pass
# main()