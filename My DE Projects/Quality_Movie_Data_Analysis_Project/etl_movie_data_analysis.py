import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node s3_data_Source
s3_data_Source_node1716971657687 = glueContext.create_dynamic_frame.from_catalog(database="movie_catalog", table_name="input", transformation_ctx="s3_data_Source_node1716971657687")

# Script generated for node data_quality_checks
data_quality_checks_node1716971864472_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        IsComplete "imdb_rating",
        ColumnValues "imdb_rating" between 8.5 and 10.3
    ]
"""

data_quality_checks_node1716971864472 = EvaluateDataQuality().process_rows(frame=s3_data_Source_node1716971657687, ruleset=data_quality_checks_node1716971864472_ruleset, publishing_options={"dataQualityEvaluationContext": "data_quality_checks_node1716971864472", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1716972301266 = SelectFromCollection.apply(dfc=data_quality_checks_node1716971864472, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1716972301266")

# Script generated for node ruleOutcomes
ruleOutcomes_node1716972198622 = SelectFromCollection.apply(dfc=data_quality_checks_node1716971864472, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1716972198622")

# Script generated for node Conditional Router
ConditionalRouter_node1716973166923 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1716972301266,
  group_filters = [GroupFilter(name = "failed_records", filters = lambda row: (bool(re.match("Failed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Failed", row["DataQualityEvaluationResult"])))))])

# Script generated for node default_group
default_group_node1716973167430 = SelectFromCollection.apply(dfc=ConditionalRouter_node1716973166923, key="default_group", transformation_ctx="default_group_node1716973167430")

# Script generated for node failed_records
failed_records_node1716973167625 = SelectFromCollection.apply(dfc=ConditionalRouter_node1716973166923, key="failed_records", transformation_ctx="failed_records_node1716973167625")

# Script generated for node drop_columns
drop_columns_node1716981998161 = ApplyMapping.apply(frame=default_group_node1716973167430, mappings=[("overview", "string", "overview", "string"), ("gross", "string", "gross", "string"), ("director", "string", "director", "string"), ("certificate", "string", "certificate", "string"), ("star4", "string", "star4", "string"), ("runtime", "string", "runtime", "string"), ("star2", "string", "star2", "string"), ("star3", "string", "star3", "string"), ("no_of_votes", "long", "no_of_votes", "int"), ("series_title", "string", "series_title", "string"), ("meta_score", "long", "meta_score", "int"), ("star1", "string", "star1", "string"), ("genre", "string", "genre", "string"), ("released_year", "string", "released_year", "string"), ("poster_link", "string", "poster_link", "string"), ("imdb_rating", "double", "imdb_rating", "decimal")], transformation_ctx="drop_columns_node1716981998161")

# Script generated for node Amazon S3
AmazonS3_node1716972738031 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1716972198622, connection_type="s3", format="json", connection_options={"path": "s3://gds-movie-data-analysis/rule_outcome/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1716972738031")

# Script generated for node Amazon S3
AmazonS3_node1716981572543 = glueContext.write_dynamic_frame.from_options(frame=failed_records_node1716973167625, connection_type="s3", format="json", connection_options={"path": "s3://gds-movie-data-analysis/bad_records/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1716981572543")

# Script generated for node redshift_load
redshift_load_node1716982427605 = glueContext.write_dynamic_frame.from_catalog(frame=drop_columns_node1716981998161, database="movie_catalog", table_name="dev_movies_imdb_movies_rating", redshift_tmp_dir="s3://noob2-gds-tempo",additional_options={"aws_iam_role": "arn:aws:iam::339713057891:role/redshift_role_new"}, transformation_ctx="redshift_load_node1716982427605")

job.commit()