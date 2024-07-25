import os
import concurrent.futures
from src.infra.etl import perform_etl
from src.infra.extract import Extract
from src.infra.s3 import S3
from src.infra.thread import Threading
from src.util.python_util import print_warning, print_message
from src.infra.write import Write  # Ensure Write class is correctly imported

try:
    from awsglue.transforms import *
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
except Exception as e:
    pass

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

glue_context = None
spark = None

try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME","connection-name-snowflake"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
except Exception as e:
    print_warning(f"Failed to initialize Glue job: {str(e)}")
    
snowflake_connection_name = args["connection_name_snowflake"]

s3_src_bucket_name = 'aws-mysql-to-sf-glue-scripts-dev-us-west-2'
s3_src_bucket_prefix = ['events_all_data/']
s3_archive_bucket_name = 'aws-mysql-to-sf-glue-scripts-archive-dev-us-west-2'
aws_region = 'us-west-2'
snowflake_database = 'PC_GLUE_ETL_DB'
snowflake_support_schema = 'demo'

schema_position = 2  # Give the position from s3 object key excluding bucket name
table_position = 3

table_join_keys = {"emerge": ["sequence"]}

def fetch_schema_metadata(schema):
    query = f"""
    SELECT 
        lower(table_schema) as table_schema,
        lower(table_name) as table_name,
        lower(column_name) as column_name,
        lower(data_type) as data_type 
    FROM 
        PC_GLUE_ETL_DB.information_schema.columns 
    WHERE 
        table_schema = '{schema}' 
        AND table_name = 'ALL_DATA'
    """
    print_message(f'query : {query}')
    # Execute the query for the current schema and return the result
    return Extract.read_sf_query(glue_context, snowflake_connection_name, snowflake_database, query)

def call_stored_procedure(schema, table='all_data'):
    try:
        post_action_query = f"""
        INSERT INTO PYX_GLUE_STAGING_DB.STAGING_SCHEMA.SURVEY_LASTLOAD (APP_ID)
        SELECT '{schema}'
        WHERE NOT EXISTS (
            SELECT 1
            FROM PYX_GLUE_STAGING_DB.STAGING_SCHEMA.SURVEY_LASTLOAD
            WHERE APP_ID = '{schema}'
        );
        CALL PYX_GLUE_STAGING_DB.STAGING_SCHEMA.SP_UPDATE_TABLE_TEMP('{schema}', '{table}');
        """ 
        Write.write_sf_dataframe(
            glue_context=glue_context,
            snowflake_connection_name=snowflake_connection_name,
            df=spark.createDataFrame([], schema=StructType([])),  # Empty DataFrame
            snowflake_database="pyx_glue_staging_db",
            schema_table="STAGING_SCHEMA.GLUE_PROCESSED_SCHEMAS",  # A dummy table name, since we're calling a SP
            object_name="CALL_SP",
            #post_action=f"CALL PYX_STAGING_DB.STAGING_SCHEMA.SP_UPDATE_TABLE_GLUE('{schema}', '{table}')"
            post_action= post_action_query
        )
        print_message(f"Called stored procedure for schema: {schema}, table: {table}")
    except Exception as e:
        print_warning(f"Failed to call stored procedure for schema: {schema}, table: {table}, Error: {str(e)}")

def main():
    print_message("Started the process...")
    print_message('Setting up config values...')

    meta_data = {
        's3_src_bucket_name': s3_src_bucket_name,
        's3_src_bucket_prefix': s3_src_bucket_prefix,
        's3_archive_bucket_name': s3_archive_bucket_name,
        'aws_region': aws_region,
        'table_join_keys': table_join_keys,
        'snowflake_connection_name': snowflake_connection_name,
        'snowflake_database': snowflake_database,
        'glue_context': glue_context,
        'spark': spark,
        'snowflake_support_schema': snowflake_support_schema
    }

    thread_count = int(os.cpu_count()) * 2
    print_message(f"Total thread_count: {thread_count}")

    all_files = S3.get_files_to_process(s3_src_bucket_name, s3_src_bucket_prefix, aws_region,
                                        schema_position, table_position)
    print_message(f'all_files: {all_files}')

    # Filter files that start with 'P', 'S', 'K', 'L', 'I', 'G', 'H', 'O'
    filtered_files = {k: v for k, v in all_files.items() if k[0].upper() in ['P', 'S', 'K', 'L', 'I', 'G', 'H', 'O']}
    print_message(f'filtered_files: {filtered_files}')

    schema_to_fetch_set = set()
    for schema_tbl in all_files:
        schema = schema_tbl.split('.')[0].upper()
        schema_to_fetch_set.add(schema)

    schema_to_fetch_list = list(schema_to_fetch_set)
    print_message(f"schema_to_fetch_list : {schema_to_fetch_list}")

    if not schema_to_fetch_list:
        print_warning(f"Nothing to process. all_files: {all_files}")
        return

    batch_size = 50
    for i in range(0, len(schema_to_fetch_list), batch_size):
        batch_schemas = schema_to_fetch_list[i:i + batch_size]
        print_message(f'batch_schemas: {batch_schemas}')

        # Clear sf_meta_data for the new batch
        sf_meta_data = spark.createDataFrame([], schema="table_schema STRING, table_name STRING, column_name STRING, data_type STRING")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit tasks for each schema in parallel
            schema_metadata_futures = {executor.submit(fetch_schema_metadata, schema): schema for schema in batch_schemas}

            # Process the results as they become available
            for future in concurrent.futures.as_completed(schema_metadata_futures):
                schema = schema_metadata_futures[future]
                try:
                    schema_meta_data = future.result()
                    sf_meta_data = sf_meta_data.union(schema_meta_data)
                except Exception as exc:
                    print(f'Exception occurred while fetching metadata for schema {schema}: {exc}')

        sf_meta_data = sf_meta_data.toDF(*[col_name.lower() for col_name in sf_meta_data.columns])

        unique_df = sf_meta_data.dropDuplicates(["table_schema", "table_name"])
        formatted_df = unique_df.withColumn("formatted_name", concat_ws(".", col("table_schema"), col("table_name")))
        available_sf_table = set(formatted_df.select("formatted_name").rdd.flatMap(lambda x: x).collect())
        meta_data['available_sf_table'] = available_sf_table

        print_message(f"available_sf_table {available_sf_table}")

        # Lowercase all column names
        for column_t in sf_meta_data.columns:
            sf_meta_data = sf_meta_data.withColumn(column_t, lower(col(column_t)))

        # Filter only the files that belong to the current batch
        batch_filtered_files = {k: v for k, v in all_files.items() if k.split('.')[0].upper() in batch_schemas}
        print_message(f"batch_filtered_files {batch_filtered_files}")
        if batch_filtered_files:
            print_message(f'Started thread. Total tables : {len(batch_filtered_files)}')
            try:
                Threading.parallelism(perform_etl, thread_count, batch_filtered_files, meta_data, sf_meta_data)
            except Exception as e:
                print_warning(f"Error in perform_etl: {str(e)}")
            finally:
                print_message('Completed thread...')
        
                with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
                    futures = [executor.submit(call_stored_procedure, schema) for schema in schema_to_fetch_set]
                    for future in concurrent.futures.as_completed(futures):
                        pass
        else:
            print_warning(f"Nothing to process. batch_filtered_files: {batch_filtered_files}")

    print_message("Completed the process...")

if __name__ == '__main__':
    main()
