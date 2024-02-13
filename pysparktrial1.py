import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_replace, split, when

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

## Splitting the category column into separate layers
cleaned_data = cleaned_data\
    .withColumn("category", split(cleaned_data["category"], "\\|"))\
    .withColumn('category_layer_1', cleaned_data['category'][0])\
    .withColumn('category_layer_2', cleaned_data['category'][1])\
    .withColumn('category_layer_3', cleaned_data['category'][2])\
    .withColumn('category_layer_4', cleaned_data['category'][3])\
    .withColumn('category_layer_5', cleaned_data['category'][4])\
    .drop('category')

## Calculate percentages of bad reviews at the product level
bad_reviews_percentage = (F.sum(F.when(cleaned_data['overall_rating'] < 3.0, 1).otherwise(0)) / F.count('*') * 100).alias('bad_reviews_percentage')
cleaned_data = cleaned_data.withColumn('bad_reviews_percentage', bad_reviews_percentage)

## Identify products with overall ratings above 4.0
above_4_ratings = F.when(cleaned_data['overall_rating'] > 4.0, True).otherwise(False).alias('above_4_ratings')
cleaned_data = cleaned_data.withColumn('above_4_ratings', above_4_ratings)

## Identify products with ratings both above 4.0 and below 3.0
above_4_below_3 = F.when((cleaned_data['overall_rating'] > 4.0) | (cleaned_data['overall_rating'] < 3.0), True).otherwise(False).alias('above_4_below_3')
cleaned_data = cleaned_data.withColumn('above_4_below_3', above_4_below_3)

## Establish product hierarchy by identifying top performers
top_performers = cleaned_data\
    .groupBy('category_layer_1', 'category_layer_2', 'category_layer_3', 'category_layer_4', 'category_layer_5')\
    .agg(F.avg('overall_rating').alias('average_rating'), F.count('*').alias('product_count'))\
    .orderBy(F.desc('average_rating'), F.desc('product_count'))

## Merge all results into a single DataFrame
final_df = cleaned_data.join(top_performers, ['category_layer_1', 'category_layer_2', 'category_layer_3', 'category_layer_4', 'category_layer_5'], 'left_outer')

## Write results to S3 as a single Parquet file
final_df.write.parquet(s3_output_path, mode="overwrite")

job.commit()
