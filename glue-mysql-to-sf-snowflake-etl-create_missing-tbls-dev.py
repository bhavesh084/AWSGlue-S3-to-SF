import os
from datetime import datetime, timedelta
from src.infra.extract import Extract
from src.infra.thread import Threading
from src.infra.missing_table import perform_missing_table
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
    support_schema = "demo"

    missing_table_diff = """
    SELECT DISTINCT 
        mysql_table_schema AS schema_name,
        mysql_table_name AS table_name 
    FROM 
        PYX_GLUE_STAGING_DB.STAGING_SCHEMA.mysql_tbl_name a 
        LEFT JOIN PC_GLUE_ETL_DB.information_schema.tables b 
            ON UPPER(a.mysql_table_schema) = UPPER(b.table_schema) 
            AND UPPER(a.mysql_table_name) = UPPER(b.table_name) 
    WHERE 
        b.table_schema IS NULL ;
    """

    meta_data = {
        'aws_region': aws_region,
        'snowflake_connection_name': snowflake_connection_name,
        'snowflake_database': snowflake_database,
        'glue_context': glue_context,
        'spark': spark,
        'mysql_connection_name': mysql_connection_name
    }

    thread_count = os.cpu_count()
    print_message(f"Total thread_count: {thread_count}")

    sf_flag_df = Extract.read_sf_query(glue_context, snowflake_connection_name, snowflake_database, missing_table_diff)
    sf_flag_df = sf_flag_df.toDF(*[col_name.lower() for col_name in sf_flag_df.columns])

    missing_schema_table = sf_flag_df.distinct().select("schema_name", "table_name").collect()

    load_data_dict = {}

    for schema_table_row in missing_schema_table:
        schema_name = schema_table_row['schema_name']
        table_name = schema_table_row['table_name']
        table_list = load_data_dict.get(schema_name, [])
        table_list.append(f"{schema_name}.{table_name}")
        load_data_dict[schema_name] = table_list

    print_message(f"load_data_dict {load_data_dict}")

    if load_data_dict:
        exception_lists = []
        for schema, tables in load_data_dict.items():
            print_message(f'Create schema if not exists schema_name: {schema}')
            schema_name = schema
            temp_schema_table = f'"{support_schema.upper()}"."{schema_name.upper()}_MISSING_TEMP"'
            temp_post_action = f'CREATE SCHEMA IF NOT EXISTS "{schema_name.upper()}"; DROP TABLE IF EXISTS {temp_schema_table};'
            temp_df = spark.createDataFrame([{"id": 1}])
            Write.write_sf_dataframe(glue_context, snowflake_connection_name, temp_df, snowflake_database,
                                     temp_schema_table, temp_schema_table,
                                     None, temp_post_action)

            print_message(f'Started thread for schema {schema}. Total tables : {len(tables)}')
            exception_list = Threading.parallelism_for_missing_table(perform_missing_table, thread_count, tables,
                                                                     meta_data)

            exception_lists.extend(exception_list)
            print_message(f'Completed threads for schema {schema}... and tables {tables}')
        if len(exception_lists) > 0:
            print_error(f"Got {len(exception_lists)} exceptions")
            raise Exception(f"Got {len(exception_lists)} exceptions", exception_lists)
        else:
            print_message("All tasks are executed successfully")
    else:
        print_warning(f"Nothing to process. load_data_dict: {load_data_dict}")

    print_message("Completed the process...")

if __name__ == '__main__':
    main()
