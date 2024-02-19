import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import split, col, when, udf
from pyspark.sql.types import FloatType, StringType, IntegerType, DoubleType
from awsglue.job import Job

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Create a dynamic frame from the Glue Data Catalog
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="amazon-sales-sk",
    table_name="cleanedfiles",
    transformation_ctx="read_from_glue_catalog"
)

# Convert the dynamic frame to a DataFrame
df = dynamic_frame.toDF()

# Handle null values by replacing them with zeros
df = df.na.fill(0)

# Extract brand name from the first word of the product_name
df = df.withColumn("brand_name", split(col("product_name"), " ")[0])

# Convert discount_percentage to double and remove % symbol
df = df.withColumn("discount_percentage(%)", col("discount_percentage").substr(1, 2).cast(DoubleType()))

# Convert rating to double and handle cases where it is empty or not a valid number
def parse_rating(rating):
    try:
        return float(rating)
    except ValueError:
        return None

parse_rating_udf = udf(parse_rating, DoubleType())

df = df.withColumn("rating_value", parse_rating_udf(col("rating")))

# Calculate above_4_rating, 3to4_rating, and bad_review_percentage
df = df.withColumn("above_4_rating", when(col("rating") > 4, 1).otherwise(0)) \
    .withColumn("3to4_rating", when((col("rating") >= 3) & (col("rating") <= 4), 1).otherwise(0)) \
    .withColumn("bad_review", when((col("above_4_rating") + col("3to4_rating")) == 0, 1).otherwise(0)) \

# Calculate top performers
from pyspark.sql.functions import desc
top_performers_list = df.groupBy("product_id").count().orderBy(desc("count")).limit(10).select("product_id").collect()
top_performers_list = [row.product_id for row in top_performers_list]
df = df.withColumn("top_performer", col("product_id").isin(top_performers_list).cast(IntegerType()))

# Drop unnecessary columns
drop_columns = ['rating_value','discount_percentage','user_id','category','product_name','user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'about_product']
df_final = df.drop(*drop_columns)

# Write the transformed data back to S3
df_final.coalesce(1).write.mode("overwrite").parquet("s3://amazonsales-capstone-sk/transformed/")

job.commit()



output: {
  "product_id": "B08MVSGXMY",
  "discounted_price": 1498,
  "actual_price": 2300,
  "rating": "3.8",
  "rating_count": "95",
  "brand_name": "Crompton",
  "discount_percentage(%)": 35,
  "above_4_rating": 0,
  "3to4_rating": 1,
  "bad_review": 0,
  "top_performer": 0
}
{
  "product_id": "B014SZO90Y",
  "discounted_price": 266,
  "actual_price": 315,
  "rating": "4.5",
  "rating_count": "28,030",
  "brand_name": "Duracell",
  "discount_percentage(%)": 16,
  "above_4_rating": 0,
  "3to4_rating": 1,
  "bad_review": 0,
  "top_performer": 0
}
{
  "product_id": "B01KK0HU3Y",
  "discounted_price": 899,
  "actual_price": 1499,
  "rating": "4.2",
  "rating_count": "23,174",
  "brand_name": "HP",
  "discount_percentage(%)": 40,
  "above_4_rating": 0,
  "3to4_rating": 1,
  "bad_review": 0,
  "top_performer": 0
}
{
  "product_id": "B075ZTJ9XR",
  "discounted_price": 269,
  "actual_price": 650,
  "rating": "4.4",
  "rating_count": "35,877",
  "brand_name": "AmazonBasics",
  "discount_percentage(%)": 59,
  "above_4_rating": 0,
  "3to4_rating": 1,
  "bad_review": 0,
  "top_performer": 0
}
{
  "product_id": "B07DKZCZ89",
  "discounted_price": 119,
  "actual_price": 499,
  "rating": "4.3",
  "rating_count": "15,032",
  "brand_name": "Gizga",
  "discount_percentage(%)": 76,
  "above_4_rating": 0,
  "3to4_rating": 1,
  "bad_review": 0,
  "top_performer": 0
}


------------------------------------------------------------------------------------------
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, translate, expr, count, sum, desc
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.window import Window

# Create a SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Define source and target paths
s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles"
s3_output_path = "s3://amazonsales-capstone-sk/transformed/"

# Read data from S3
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database="amazonsales-sk-capstone", table_name="cleanedfiles", transformation_ctx="dynamic_frame")

# Convert DynamicFrame to DataFrame
df_original = dynamic_frame.toDF()

# Function to remove symbols from column values
def remove_symbols(column):
    return translate(column, "₹,%", "")

# Function to change column names and remove symbols
def rename_and_clean_columns(df):
    df = df.withColumnRenamed("discounted_price", "discounted_price(₹)") \
           .withColumnRenamed("actual_price", "actual_price(₹)") \
           .withColumn("discounted_price(₹)", remove_symbols(col("discounted_price(₹)"))) \
           .withColumn("actual_price(₹)", remove_symbols(col("actual_price(₹)")))
    df = df.withColumn("discounted_price(₹)", df["discounted_price(₹)"].cast(IntegerType())) \
           .withColumn("actual_price(₹)", df["actual_price(₹)"].cast(IntegerType()))
    return df

# Function to remove % symbol from discount_percentage column
def clean_discount_percentage(df):
    df = df.withColumn("discount_percentage", remove_symbols(col("discount_percentage"))) \
           .withColumn("discount_percentage", col("discount_percentage").cast(FloatType()))
    return df

# Function to replace null values with 'N.A.'
def replace_null_values(df):
    df = df.fillna("N.A.")
    return df

# Function to drop duplicate rows based on specified columns
def drop_duplicate_rows(df):
    df = df.dropDuplicates(["product_id", "discounted_price(₹)", "actual_price(₹)", "rating", "rating_count"])
    return df

# Function to change data formats
def change_data_formats(df):
    df = df.withColumn("rating", col("rating").cast(FloatType())) \
           .withColumn("rating_count", remove_symbols(col("rating_count"))) \
           .withColumn("rating_count", col("rating_count").cast(IntegerType()))
    return df

# Apply all business logic transformations
def apply_business_logic(df):
    df = rename_and_clean_columns(df)
    df = clean_discount_percentage(df)
    df = replace_null_values(df)
    df = drop_duplicate_rows(df)
    df = change_data_formats(df)
    df = df.withColumn("above_4_rating", expr("IF(rating > 4, 1, 0)")) \
           .withColumn("3to4_rating", expr("IF(rating >= 3 AND rating <= 4, 1, 0)"))
    df = df.withColumn("brandname", expr("translate(substring_index(category, '|', 1), '_', ' ')"))
    windowSpec = Window.partitionBy("product_id")
    df = df.withColumn("bad_review_percentage", (count("product_id").over(windowSpec) - 1) / count("product_id").over(windowSpec) * 100)
    df = df.withColumn("rank", expr("rank() over (order by rating_count desc)"))
    top_performers_list = df.select("product_id").distinct().limit(10)
    df = df.withColumn("top_performer", expr("IF(product_id in ({0}), 1, 0)".format(','.join([str(row.product_id) for row in top_performers_list.collect()]))))
    return df

# Apply basic data cleaning and transformation operations
df_cleaned = apply_business_logic(df_original)

# Write results to S3 as a single Parquet file
df_cleaned.coalesce(1).write.parquet(s3_output_path, mode="overwrite")
