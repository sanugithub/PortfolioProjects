import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

subsDf = glueContext.create_dynamic_frame.from_catalog(
    database="telecom-datalog",
    table_name="gds_telecom_data_glue",
    transformation_ctx="s3_input_new"
    )

subsDf.printSchema()
sparkSubsDf = subsDf.toDF()
sparkSubsDf.show()
print(sparkSubsDf.count())

job.commit()