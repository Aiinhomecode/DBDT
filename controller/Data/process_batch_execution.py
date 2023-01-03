import pandas as pd
from db_connection import conn
from .make_db_connection import sqlserver_con, mysql_con
from pandasql import sqldf
from datetime import datetime
from time import sleep

def fetch_process_details(process_master_id):
    process_details = pd.read_sql(f"EXEC [GET_PROCESS_DETAILS] {process_master_id}", con = conn)
    target = process_details.iloc[0].TERGATE
    source_tables = process_details.iloc[0].SOURSE_TABLE.split(',')
    target_table = process_details.iloc[0].TERGATE_NAME
    host = process_details.iloc[0].HOST
    port = process_details.iloc[0].PORT
    user = process_details.iloc[0].USER
    password =process_details.iloc[0].PASSWORD
    database = process_details.iloc[0].DATABASE
    return target, source_tables, target_table, host, port, user, password, database

def get_db_connection(target, **credentials):
    conn_obj = {}
    if target.lower() == 'sqlserver':
        conn_obj = sqlserver_con(credentials['host'], credentials['port'], credentials['user'], credentials['password'], credentials['database'])
    elif target.lower() == 'mysql':
        conn_obj = mysql_con(credentials['host'], credentials['port'], credentials['user'], credentials['password'], credentials['database'])
    return conn_obj

def get_runable_processes():
    runable_processes = pd.read_sql("EXEC GET_RUNABLE_PROCESSES", con = conn)
    return runable_processes

def update_process_status(process_master_id, status):
    try:
        connection = conn.raw_connection()
        cursor = connection.cursor()
        cursor.execute(f"EXEC UPDATE_PROCESS_STATUS {process_master_id}, {status}")
    except Exception as e:
        raise Exception("Failed to change status.")
    finally:
        cursor.close()
        connection.commit()
        connection.close()

def logging_process_history(**data):
    try:
        connection = conn.raw_connection()
        cursor = connection.cursor()
        cursor.execute(f"EXEC LOG_EXECUTION_REPORT {data['process_master_id']},'{data['execution_status']}','{data['execution_start']}','{data['execution_end']}',{data['records_effected']},'{data['error_message']}'")
    finally:
        cursor.close()
        connection.commit()
        connection.close()
    
def get_process_queries(process_master_id):
    queries = pd.read_sql(f'EXEC [GET_PROCESS_QUERIES] {process_master_id}', con = conn)
    return queries

def push_processed_data(connection, table, final_result):
    final_result.to_sql(table, con = connection, index = False, if_exists = 'append')

def execute_process(connection, **data):
    for table in data['source_tables']:
        globals()[table] = pd.read_sql_table(table, con = connection)
    
    process_queries = get_process_queries(data['process_master_id'])
    index = 0
    for query in process_queries.itertuples():
        globals()["Query"+str(index)] = sqldf(query)
        index+=1
    
    final_result = globals()["Query"+str(len(process_queries))]
    push_processed_data(connection, data['target_table'], final_result)
    
    return len(final_result)

def main():
    
    runable_processes = get_runable_processes()
    for process in runable_processes.itertuples():
        try:
            execution_start = datetime.now()
            process_master_id = process.PROCESS_MASTER_ID
            update_process_status(data['process_master_id'], 'RUNNING')
            target, source_tables, target_table, host, port, user, password, database = fetch_process_details(process_master_id)
            credentials = {
                'host':host,
                'port':port,
                'user':user,
                'password':password,
                'database':database,
            }
            conn_obj = get_db_connection(target, **credentials)
            if conn_obj['status']:
                data = {'source_tables':source_tables, 'target_table':target_table, 'process_master_id':process_master_id}
                records_effected = execute_process(conn_obj['connection'], **data)
                execution_end = datetime.now()
                data = {
                'process_master_id': process_master_id,
                'execution_status': 'Success',
                'execution_start': execution_start,
                'execution_end': execution_end,
                'records_effected': records_effected,
                'error_message': str(e)
            }
                status = 'SUCCESS'
            else:
                raise Exception('Failed to connect')
        except Exception as e:
            execution_end = datetime.now()
            data = {
                'process_master_id': process_master_id,
                'execution_status': 'Failed',
                'execution_start': execution_start,
                'execution_end': execution_end,
                'records_effected': 0,
                'error_message': str(e)
            }
            status = 'FAILD'
        finally:
            update_process_status(data['process_master_id'], status)
            logging_process_history(**data)

while True:
    try:
        main()
        sleep(1)
    except Exception as e:
        sleep(1)
        pass