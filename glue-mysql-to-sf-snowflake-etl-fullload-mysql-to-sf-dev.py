import os
import sys
from datetime import datetime

from src.infra.extract import Extract
from src.infra.thread import Threading
from src.infra.validation import perform_validation
from src.infra.write import Write
from src.util.python_util import print_warning, print_message, print_error

try:
    from awsglue.transforms import *
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
except Exception as e:
    print_error(str(e))

from pyspark.sql.functions import *

glue_context = None
spark = None

try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "connection-name-mysql", "connection-name-snowflake"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
except Exception as e:
    print_error(f"Failed to initialize Glue job or retrieve arguments: {str(e)}")
    raise

mysql_connection_name = args["connection_name_mysql"]
snowflake_connection_name = args["connection_name_snowflake"]

def main():
    print_message("Started the process...")
    print_message('Setting up config values...')

    aws_region = 'us-west-2'
    snowflake_database = 'PC_GLUE_ETL_DB'
    flag_schema_table = 'pyx_glue_staging_db.STAGING_SCHEMA.SURVEY_LASTLOAD'
    validate_table_names = [
        'categories', 'benchmarks', 'emerge', 'resp_list', 'resp_questions',
        'surv_options', 'surv_questions', 'prefill_data', 'all_data', 'managers_tree',
        'segments', 'users', 'lexical_themes', 'lexical_keywords', 'category_qid',
        'settings', 'benchmark_data', 'ss_user_apps', 'statuses'
        ]

    meta_data = {
        'aws_region': aws_region,
        'flag_schema_table': flag_schema_table,
        'snowflake_connection_name': snowflake_connection_name,
        'snowflake_database': snowflake_database,
        'glue_context': glue_context,
        'spark': spark,
        'mysql_connection_name': mysql_connection_name
    }

    thread_count = os.cpu_count()
    print_message(f"Total thread_count: {thread_count}")

    query = f"SELECT * FROM {flag_schema_table} where flag=True "
    sf_flag_df = Extract.read_sf_query(glue_context, snowflake_connection_name, snowflake_database, query)
    sf_flag_df = sf_flag_df.toDF(*[col_name.lower() for col_name in sf_flag_df.columns])

    validate_sf_schema = sf_flag_df.distinct().select("app_id").collect()

    load_data_dict = {}

    for schema_row in validate_sf_schema:
        app_id = schema_row['app_id'].lower()
        table_list = load_data_dict.get(app_id, [])
        for table in validate_table_names:
            table_list.append(f'"{app_id}".{table}')
        load_data_dict[app_id] = table_list

    print_message(f"load_data_dict {load_data_dict}")

    if load_data_dict:
        exception_lists = []
        for schema, tables in load_data_dict.items():
            print_message(f'Started thread for schema {schema}. tables ---- {tables} ----- Total tables : {len(tables)}')
            exception_list = Threading.parallelism_for_validation(perform_validation, thread_count, tables, meta_data)
            
            if len(exception_list) == 0:
                curr_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                temp_schema_table = f'"{schema}"._temp_exec_tbl'
                
                post_action = f"""
                    update {flag_schema_table} 
                    set flag=False, fullload_timestamp='{curr_timestamp}' 
                    where app_id='{schema.upper()}'; 
                    DROP TABLE IF EXISTS {temp_schema_table.upper()};
                    CALL PYX_GLUE_STAGING_DB.STAGING_SCHEMA.SP_UPDATE_TABLE_TEMP('{schema.upper()}', 'ALL_DATA');
                """
                
                print_message(f"Updating flag to False and fullload_timestamp as {curr_timestamp}")
                temp_df = spark.createDataFrame([{"id": 1}])
                try:
                    Write.write_sf_dataframe(glue_context, snowflake_connection_name, temp_df, snowflake_database,
                                         temp_schema_table, temp_schema_table,
                                         None, post_action)
                    print_message(f"Updated flag to False and fullload_timestamp as {curr_timestamp}")
                except Exception as e:
                    if "net.snowflake.client.jdbc.SnowflakeSQLException: Execution error in store procedure SP_UPDATE_TABLE_TEMP" in str(e) and "is too long and would be truncated" in str(e):
                        exception_message = f"Warning: String too long for stored procedure SP_UPDATE_TABLE_TEMP for schema: {schema}, table: {temp_schema_table}. Exception: {str(e)}"
                        print_warning(exception_message)
                    else:
                        exception_message = f"Error writing data frame for schema: {schema}, table: {temp_schema_table}. Exception: {str(e)}"
                        print_warning(exception_message)
                    
                    exception_lists.append(exception_message)
                    print_warning(exception_message)
                    
                

            exception_lists.extend(exception_list)
            print_message(f'Completed threads for schema {schema}... and tables {tables}')
        
        if len(exception_lists) > 0:
            print_warning(f"Got {len(exception_lists)} exceptions")
            #raise Exception(f"Got {len(exception_lists)} exceptions", exception_lists)
        else:
            print_message("All tasks are executed successfully")
    else:
        print_warning(f"Nothing to process. source_target_dict: {load_data_dict}")

    print_message("Completed the process...")

if __name__ == '__main__':
    main()
