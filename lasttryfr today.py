import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, translate, expr, count
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
    df = df.drop('user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link','about_product')\
           .withColumnRenamed("discounted_price", "discounted_price(₹)") \
           .withColumnRenamed("actual_price", "actual_price(₹)") \
           .withColumn("discounted_price(₹)", remove_symbols(col("discounted_price(₹)"))) \
           .withColumn("actual_price(₹)", remove_symbols(col("actual_price(₹)")))
    df = df.withColumn("discounted_price(₹)", df["discounted_price(₹)"].cast(IntegerType())) \
           .withColumn("actual_price(₹)", df["actual_price(₹)"].cast(IntegerType()))
    return df

# Function to split category column into separate layers
def split_category(df):
    split_col = expr("split(category, '\\|')")
    df = df.withColumn("category_layer_1", split_col.getItem(0)) \
           .withColumn("category_layer_2", split_col.getItem(1)) \
           .withColumn("category_layer_3", split_col.getItem(2)) \
           .withColumn("category_layer_4", split_col.getItem(3)) \
           .withColumn("category_layer_5", split_col.getItem(4))
    return df.drop("category")

# Function to clean discount_percentage column
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
    df = split_category(df)
    df = clean_discount_percentage(df)
    df = replace_null_values(df)
    df = drop_duplicate_rows(df)
    df = change_data_formats(df)
    return df

# Apply basic data cleaning and transformation operations
df_cleaned = apply_business_logic(df_original)

# Apply additional business logic transformations
df_above_4 = df_cleaned.filter(col("rating") > 4.0)
df_above_4_below_3 = df_cleaned.filter((col("rating") > 4.0) & (col("rating") < 3.0))

windowSpec = Window.partitionBy("product_id")
df_with_bad_review_percentage = df_cleaned.withColumn("bad_review_percentage", (count("product_id").over(windowSpec) - 1) / count("product_id").over(windowSpec) * 100)

df_ranked_by_rating_count = df_cleaned.withColumn("rank", expr("rank() over (order by rating_count desc)"))

# Ensure all DataFrames have the same number of columns
num_columns = df_cleaned.columns
df_above_4 = df_above_4.select(*num_columns)
df_above_4_below_3 = df_above_4_below_3.select(*num_columns)
df_with_bad_review_percentage = df_with_bad_review_percentage.select(*num_columns)
df_ranked_by_rating_count = df_ranked_by_rating_count.select(*num_columns)

# Combine DataFrames using union
final_df = df_above_4.union(df_above_4_below_3).union(df_with_bad_review_percentage).union(df_ranked_by_rating_count)

# Write results to S3 as a single Parquet file
final_df.write.parquet(s3_output_path, mode="overwrite")
