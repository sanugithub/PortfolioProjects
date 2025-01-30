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

# Script generated for node S3 Source Glue DC
S3SourceGlueDC_node1716449408958 = glueContext.create_dynamic_frame.from_catalog(database="glue_bt_db", table_name="bt_input", transformation_ctx="S3SourceGlueDC_node1716449408958")

# Script generated for node Deduplicating and Filtering
SqlQuery1014 = '''
with cte as (
select *,
row_number() over(partition by transaction_id order by transaction_date) as t_rank,
count(transaction_id) over(partition by transaction_id) as t_count
from s3_bt_source
where transaction_id is not null
)
select * from cte where t_rank=1
'''
DeduplicatingandFiltering_node1716449419417 = sparkSqlQuery(glueContext, query = SqlQuery1014, mapping = {"s3_bt_source":S3SourceGlueDC_node1716449408958}, transformation_ctx = "DeduplicatingandFiltering_node1716449419417")

# Script generated for node S3 Target
S3Target_node1716449427058 = glueContext.write_dynamic_frame.from_options(frame=DeduplicatingandFiltering_node1716449419417, connection_type="s3", format="glueparquet", connection_options={"path": "s3://bank-transactions-gds/output/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3Target_node1716449427058")

job.commit()