import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, translate, expr, count, row_number
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import IntegerType, FloatType

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
    return df

# Apply basic data cleaning and transformation operations
df_cleaned = apply_business_logic(df_original)

# Calculate above_4_rating and 3<rating>4 at the row level
df_cleaned = df_cleaned.withColumn("above_4_rating", (col("rating") > 4.0).cast(IntegerType()))
df_cleaned = df_cleaned.withColumn("3to4_rating", ((col("rating") >= 3.0) & (col("rating") < 4.0)).cast(IntegerType()))

# Calculate bad_review_percentage
windowSpec = Window.partitionBy("product_id")
df_cleaned = df_cleaned.withColumn("bad_review_percentage", (count("product_id").over(windowSpec) - 1) / count("product_id").over(windowSpec) * 100)

# Extract sub_category_level from category column
df_cleaned = df_cleaned.withColumn("sub_category_level", expr("split(category, '\\\\|')"))

# Assume product hierarchy is the first three levels of the category
df_cleaned = df_cleaned.withColumn("product_hierarchy", expr("concat_ws(' > ', sub_category_level[0], sub_category_level[1], sub_category_level[2])"))

# Write results to S3 as a single Parquet file
df_cleaned.coalesce(1).write.parquet(s3_output_path, mode="overwrite")
