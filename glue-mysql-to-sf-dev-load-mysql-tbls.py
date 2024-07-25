import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node MySQL
MySQL_node1706567347021 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "INFORMATION_SCHEMA.TABLES",
        "connectionName": "glue-mysql-to-sf-mysql-dev",
    },
    transformation_ctx = "MySQL_node1706567347021"
)

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
     Table_Schema as MYSQL_Table_Schema,
     Table_Name as MYSQL_Table_Name,
     '' as SF_TABLE_SCHEMA,
     '' as SF_TABLE_NAME,
    now() as SYNCED_AT
     FROM myDataSource
     WHERE table_schema <> 'information_schema'
     and   Table_Name in ('resp_questions','categories','statuses','resp_list','all_data','surv_options','lexical_themes','new_action_plans','new_action_plan_items') and Table_type <>'VIEW'
'''
SQLQuery_node1719211249515 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":MySQL_node1706567347021}, transformation_ctx = "SQLQuery_node1719211249515")

# Script generated for node Snowflake
Snowflake_node1719211865586 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1719211249515, connection_type="snowflake", connection_options={"autopushdown": "on", "dbtable": "MYSQL_TBL_NAME", "connectionName": "glue-snowflake-dev-conn", "preactions": "CREATE TABLE IF NOT EXISTS STAGING_SCHEMA.MYSQL_TBL_NAME (qid string, q_type string, q_text string, q_condition string, quid string); TRUNCATE TABLE STAGING_SCHEMA.MYSQL_TBL_NAME;", "sfDatabase": "PYX_GLUE_STAGING_DB", "sfSchema": "STAGING_SCHEMA"}, transformation_ctx="Snowflake_node1719211865586")

job.commit()
