import pandas as pd
from sqlalchemy import null
from db_connection import conn
from controller.Data.make_db_connection import sqlserver_con, mysql_con
from pandasql import sqldf
from datetime import datetime
from time import sleep
import numpy as np

def fetch_process_details(process_master_id):
    try:
        print('su code')
        process_details = pd.read_sql(f"EXEC [GET_PROCESS_DETAILS] {process_master_id}", con = conn)
        target = process_details.iloc[0].TARGET
        source_tables = process_details.iloc[0].SOURSE_TABLE.split(',')
        target_table = process_details.iloc[0].TERGATE_NAME
        host = process_details.iloc[0].HOST
        port = process_details.iloc[0].PORT
        user = process_details.iloc[0].USER
        password =process_details.iloc[0].PASSWORD
        database = process_details.iloc[0].DATABASE
        return target, source_tables, target_table, host, port, user, password, database
    except Exception as e:
        raise Exception("Failed to fetch processor details.")

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
        cursor.execute(f"EXEC UPDATE_PROCESS_STATUS {process_master_id}, '{status}'")
    except Exception as e:
        print(str(e))
        raise Exception("Failed to change status.")
    finally:
        cursor.close()
        connection.commit()
        connection.close()

def logging_process_history(**data):
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
    
def get_process_queries(process_master_id):
    queries = pd.read_sql(f'EXEC [GET_PROCESS_QUERIES] {process_master_id}', con = conn)
    return queries

def push_processed_data(connection, table, final_result):
    final_result.to_sql(table, con = connection, index = False, if_exists = 'append')

def execute_process(connection, **data):

    raw_conn = connection.raw_connection()
    cursor = raw_conn.cursor()
    # for table in data['source_tables']:
    #     globals()[table] = pd.read_sql_table(table, con = connection)
    
    process_queries = get_process_queries(data['process_master_id'])
    index = 1
    for query in process_queries.itertuples():
        try:
            execution_start = datetime.now()
            log_data = {
                'process_master_id': data['process_master_id'],
                'subprocess_id' : query.PROCESS_TRANSACTION_ID,
                'execution_report_id' : query.EXECUTION_REPORT_ID,
                'execution_status': 'Running',
                'execution_start': execution_start,
                'execution_end': null,
                'records_effected': null,
                'error_message': ''
            }
            logging_process_history(**log_data)
            if index > 1:
                select_query = query.QUERY.replace("Query","#Query")
            else:
                select_query = query.QUERY
            globals()["result"+str(index)] = cursor.execute(select_query).fetchall()
            columns = []
            columns_with_datatype = []
            datatypes = {
                "int" : "bigint",
                "str" : "nvarchar(max)",
                "float" : "float",
                "Decimal" : "float",
                "datetime" : "datetime",
                "date" : "date",
                "time" : "time"
            }
            for column in cursor.description:
                columns.append(column[0])
                columns_with_datatype.append("["+column[0]+"] "+datatypes[column[1].__name__])
            if len(globals()["result"+str(index)]) > 0:
                globals()['result_df'+str(index)] = pd.DataFrame(np.array(globals()["result"+str(index)]), columns=columns)
            else:
                globals()['result_df'+str(index)] = pd.DataFrame( columns=columns)
            if len(process_queries)>index:
                globals()['json_result_df'+str(index)] = globals()['result_df'+str(index)].to_json(orient = 'records')
                table_query = f" create table #Query{index} ("+(",".join(columns_with_datatype))+")"
                cursor.execute(table_query)
                insert_query = f"""insert into #Query{index} ("""+(",".join(columns))+") select * from openjson(?) with ("+(",".join(columns_with_datatype))+")"
                cursor.execute(insert_query, globals()['json_result_df'+str(index)])
            execution_end = datetime.now()
            log_data = {
                'process_master_id': data['process_master_id'],
                'subprocess_id' : query.PROCESS_TRANSACTION_ID,
                'execution_report_id' : query.EXECUTION_REPORT_ID,
                'execution_status': 'Completed',
                'execution_start': execution_start,
                'execution_end': execution_end,
                'records_effected': len(globals()['result_df'+str(index)]),
                'error_message': ''
            }
            logging_process_history(**log_data)
            index+=1
        except Exception as e:
            print(str(e))
            execution_end = datetime.now()
            log_data = {
                'process_master_id': data['process_master_id'],
                'subprocess_id' : query.PROCESS_TRANSACTION_ID,
                'execution_report_id' : query.EXECUTION_REPORT_ID,
                'execution_status': 'Failed',
                'execution_start': execution_start,
                'execution_end': execution_end,
                'records_effected': 0,
                'error_message': str(e)
            }
            logging_process_history(**log_data)
            raise Exception()
        
            

    cursor.close()
    raw_conn.commit()
    raw_conn.close()
       
    final_result = globals()["result_df"+str(len(process_queries))]
    push_processed_data(connection, data['target_table'], final_result)
    return len(final_result)

def main():
    
    runable_processes = get_runable_processes()
    for process in runable_processes.itertuples():
        try:
            execution_start = datetime.now()
            process_master_id = process.PROCESS_MASTER_ID
            update_process_status(process_master_id, 'RUNNING')
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
                
                status = 'SUCCESS'
            else:
                raise Exception('Failed to connect')
        except Exception as e:
            execution_end = datetime.now()
            
            status = 'FAILED'
        finally:
            update_process_status(process_master_id, status)
            # logging_process_history(**data)

while True:
    try:
        main()
        sleep(1)
    except Exception as e:
        sleep(1)
        pass