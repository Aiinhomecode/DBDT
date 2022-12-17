from flask import Flask
from flask_cors import CORS
from controller.Auth.user_login import user_login
from controller.Connection.ftp_test_connection import ftp_test_connection
from controller.Connection.oracle_db_test_connection import oracle_db_test_connection
from controller.Connection.azure_blob_test_connection import azure_blob_test_connection
from controller.Data.test_save_ftp_source import test_save_ftp_source
from controller.Connection.target_test_connection import target_test_connection
from controller.Data.save_target_details import save_target_details
from controller.Data.get_sources import get_sources
from controller.Data.get_targets import get_targets
from controller.Data.sources_dropdows import sources_dropdown
from controller.Data.targets_dropdown import targets_dropdown
from controller.Data.get_source_filenames import get_source_filenames
from controller.Data.get_source_data import get_source_data
from controller.Data.push_source_data_to_target import push_source_data_to_target
from controller.Data.get_Target_tables_name import get_Target_tables_name
from controller.Data.get_source_connection_details import get_source_connection_details
from controller.Data.get_transformation_sample_data import get_transformation_sample_data
from controller.Data.get_Target_table_column_value import get_Target_table_column_value
from controller.Data.get_Target_table_columns_name import get_Target_table_columns_name
from controller.Data.get_validate_query_result import get_validate_query_result
from controller.Connection.google_drive_source_save import google_drive_source_save
from controller.Connection.get_drive_details import get_drive_details
from controller.Data.get_drive_file_data import get_drive_file_data
from controller.Data.first_row_as_header import get_header_or_not
from controller.Data.get_visualisation_aggregate_colums import get_visualisation_aggregate_colums
from controller.Data.get_view_name_query import get_view_query
from controller.Data.save_query_view import save_query_view
from controller.Data.get_specific_column_name_data_type import get_Target_table_specific_columns_name_data_type
from controller.Data.test_save_azure_blob_source import test_save_azure_blob_source
from controller.Data.test_save_oracle_source import test_save_oracle_source
from controller.Data.test_save_mongo_db_source import test_save_mongo_db_source
from controller.Connection.mongo_db_test_connection import mongo_db_test_connection
from controller.Data.get_log_details import get_log_details
from controller.Data.save_db_source import save_db_source
from controller.Data.upload_localsource_files import upload_localsource_files
from controller.Data.existing_localsource_dropdown import existing_localsource_dropdown
from controller.Data.get_target_connection_details import get_target_connection_details
from controller.Data.save_process_data import insert_process_data
from controller.Data.details_GET_PROCESS_MASTER import details_GET_PROCESS_MASTER


# import handler.err_handler

app = Flask(__name__)

CORS(app)

BASE_URL = '/dbdt/api'


# User Login
@app.route(BASE_URL+"/auth/login", methods=['POST'])
def login():

    return user_login()

# FTP Test connection
@app.route(BASE_URL+"/test-connection/ftp", methods = ['POST','GET'])
def ftp_connection_test():
    return ftp_test_connection()

# Oracle Test connection
@app.route(BASE_URL+"/test-connection/oracle", methods = ['POST','GET'])
def oracle_db_test_connection_():
    return oracle_db_test_connection()

# Azure Test connection
@app.route(BASE_URL+"/test-connection/azure", methods = ['POST','GET'])
def azure_blob_test_connection_():
    return azure_blob_test_connection()     

# Mongo DB Test connection
@app.route(BASE_URL+"/test-connection/mongo_db", methods = ['POST','GET'])
def mongo_db_test_connection_():
    return mongo_db_test_connection() 

# FTP Source Test Save
@app.route(BASE_URL+"/source-save/ftp", methods = ['POST','GET'])
def ftp_save_test():
    return test_save_ftp_source()

# ORACLE Source Test Save
@app.route(BASE_URL+"/source-save/oracle", methods = ['POST','GET'])
def test_save_oracle_source_():
    return test_save_oracle_source()

# Azure Blob Source Test Save
@app.route(BASE_URL+"/source-save/azure_blob", methods = ['POST','GET'])
def test_save_azure_blob_source_():
    return test_save_azure_blob_source()        

# Mongo DB Source Test Save
@app.route(BASE_URL+"/source-save/mongo_db", methods = ['POST','GET'])
def test_save_mongo_db_source_():
    return test_save_mongo_db_source()

#  DB Source  Save
@app.route(BASE_URL+"/source-save/db", methods = ['POST','GET'])
def save_db_source_():
    return save_db_source()

# Target Connection Test
@app.route(BASE_URL+"/test-connection/target", methods = ['POST','GET'])
def target_connection_check():
    return target_test_connection()

# Save Target Connection
@app.route(BASE_URL+"/target-save/mysql+sqlserver", methods = ['POST','GET'])
def save_target():
    return save_target_details()

# Get source list
@app.route(BASE_URL+"/get-source-list", methods = ['POST','GET'])
def get_source():
    return get_sources()

# Get target list
@app.route(BASE_URL+"/get-target-list", methods = ['POST','GET'])
def get_target():
    return get_targets()

# sources dropdown
@app.route(BASE_URL+"/sources-dropdown", methods = ['POST','GET'])
def sources_dropdown_():
    return sources_dropdown()

# target dropdown
@app.route(BASE_URL+"/targets-dropdown", methods = ['POST','GET'])
def targets_dropdown_():
    return targets_dropdown()

# get file names from source
@app.route(BASE_URL+"/get-filenames-source", methods = ['POST','GET'])
def get_source_filenames_():
    return get_source_filenames()

# get source file data
@app.route(BASE_URL+"/get-source-data", methods = ['POST','GET'])
def get_source_data_():
    return get_source_data()

# push data to target
@app.route(BASE_URL+"/push-data-target", methods = ['POST','GET'])
def push_source_data_to_target_():
    return push_source_data_to_target()

# get target table names
@app.route(BASE_URL+"/get_Target_tables_name", methods = ['POST','GET'])
def get_Target_tables_name_():
    return get_Target_tables_name()

# get source connection details
@app.route(BASE_URL+"/get_source_connection_details", methods = ['POST','GET'])
def get_source_connection_details_():
    return get_source_connection_details()

# get transformation table data
@app.route(BASE_URL+"/get_transformation_sample_data", methods = ['POST','GET'])
def get_transformation_sample_data_():
    return get_transformation_sample_data()

# get target table columns value
@app.route(BASE_URL+"/get_Target_table_column_value", methods = ['POST','GET'])
def get_Target_table_column_value_():
    return get_Target_table_column_value()

# get target table columns
@app.route(BASE_URL+"/get_Target_table_columns_name", methods = ['POST','GET'])
def get_Target_table_columns_name_():
    return get_Target_table_columns_name()

# get target table specific columns name and data type
@app.route(BASE_URL+"/get_Target_table_specific_columns_name_data_type", methods = ['POST','GET'])
def get_Target_table_specific_columns_name_data_type_():
    return get_Target_table_specific_columns_name_data_type()       

# get validation query result
@app.route(BASE_URL+"/get_validate_query_result", methods = ['POST','GET'])
def get_validate_query_result_():
    return get_validate_query_result()

# save drive details
@app.route(BASE_URL+"/google_drive_source_save", methods = ['POST','GET'])
def google_drive_source_save_():
    return google_drive_source_save()

# get drive details
@app.route(BASE_URL+"/get_drive_details", methods = ['POST','GET'])
def get_drive_details_():
    return get_drive_details()

# get drive file data
@app.route(BASE_URL+"/get_drive_file_data", methods = ['POST','GET'])
def get_drive_file_data_():
    return get_drive_file_data()

# Visualization aggregation
@app.route(BASE_URL+"/get_visualisation_aggregate_colums", methods = ['POST','GET'])
def get_visualisation_aggregate_colums_():
    return get_visualisation_aggregate_colums()

# Visualization view names
@app.route(BASE_URL+"/get_view_name_query", methods = ['POST','GET'])
def get_view_name_query_():
    return get_view_query()

# Visualization save query
@app.route(BASE_URL+"/save_query_view", methods = ['POST','GET'])
def save_query_view_():
    return save_query_view()

#get data with or without header
@app.route(BASE_URL+'/first-row-as-header',methods = ['GET'])
def get_headerData_or_not():
    return get_header_or_not()

#get log details
@app.route(BASE_URL+'/get-log-details',methods = ['GET','POST'])
def get_log_details_():
    return get_log_details()

#Local Source
@app.route(BASE_URL+'/localsource-save',methods = ['GET','POST'])
def upload_localsource_files_():
    return upload_localsource_files()

# Existing Local Source
@app.route(BASE_URL+'/existing-local-sources',methods = ['GET','POST'])
def existing_localsource_dropdown_():
    return existing_localsource_dropdown()

# Existing Local Source
@app.route(BASE_URL+'/get-target-connection-details',methods = ['GET','POST'])
def get_target_connection_details_():
    return get_target_connection_details()

# Insert Process Data
@app.route(BASE_URL+'/insert-process-data',methods = ['GET','POST'])
def save_process_data_():
    return insert_process_data()

#api for details of process_master
@app.route(BASE_URL+'/details_GET_PROCESS_MASTER',methods = ['GET','POST'])
def details_GET_PROCESS_MASTER_():
    return details_GET_PROCESS_MASTER()
    

@app.route("/")
def rr():
    return 'ok'

if __name__ == "__main__":
    app.run(debug=True,host='0.0.0.0', port=3004)