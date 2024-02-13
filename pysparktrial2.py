import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_replace, split, struct

# Define source and target paths
s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles"
s3_output_path = "s3://amazonsales-capstone-sk/transformed/"

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Read data from S3
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database="amazonsales-sk-capstone", table_name="cleanedfiles", transformation_ctx="dynamic_frame")

## Convert DynamicFrame to DataFrame
cleaned_data = dynamic_frame.toDF()

## Data cleansing and processing
cleaned_data = cleaned_data\
    .drop('user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link')\
    .withColumnRenamed('discounted_price(₹)', 'discounted_price')\
    .withColumn('discount_percentage', regexp_replace('discount_percentage', '%', '').cast('double'))

## Splitting the category column into separate layers and converting to struct type
split_category = split(cleaned_data["category"], "\\|")
cleaned_data = cleaned_data.withColumn("category_struct", struct(split_category[0], split_category[1], split_category[2], split_category[3], split_category[4]))\
    .drop("category")

## Calculate percentages of bad reviews at the product level
cleaned_data = cleaned_data.withColumn('bad_reviews_percentage', (F.sum(F.when(cleaned_data['rating'] < 3.0, 1).otherwise(0)) / F.count('*') * 100))

## Identify products with overall ratings above 4.0
cleaned_data = cleaned_data.withColumn('above_4_ratings', (cleaned_data['rating'] > 4.0).cast('boolean'))

## Identify products with ratings both above 4.0 and below 3.0
cleaned_data = cleaned_data.withColumn('above_4_below_3', ((cleaned_data['rating'] > 4.0) | (cleaned_data['rating'] < 3.0)).cast('boolean'))

## Establish product hierarchy by identifying top performers
top_performers = cleaned_data\
    .groupBy("category_struct")\
    .agg(F.avg('rating').alias('average_rating'), F.count('*').alias('product_count'))\
    .orderBy(F.desc('average_rating'), F.desc('product_count'))

## Merge all results into a single DataFrame
final_df = cleaned_data.join(top_performers, cleaned_data['category_struct'] == top_performers['category_struct'], 'left_outer')\
    .drop(top_performers['category_struct'])

## Write results to S3 as a single Parquet file
final_df.write.parquet(s3_output_path, mode="overwrite")

job.commit()
