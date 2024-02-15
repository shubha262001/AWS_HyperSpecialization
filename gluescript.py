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


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
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
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database="amazon-sales-sk", table_name="cleanedfiles", transformation_ctx="dynamic_frame")

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



==========================================================================
import sys
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
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database="amazon-sales-sk", table_name="cleanedfiles", transformation_ctx="dynamic_frame")

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


-------------------------------------------------------------------------------------

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, translate, expr, count
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
import sys
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
    return df

# Apply basic data cleaning and transformation operations
df_cleaned = apply_business_logic(df_original)

# Apply additional business logic transformations
df_above_4 = df_cleaned.filter(col("rating") > 4.0)
df_above_4_below_3 = df_cleaned.filter((col("rating") > 4.0) & (col("rating") < 3.0))

windowSpec = Window.partitionBy("product_id")
df_with_bad_review_percentage = df_cleaned.withColumn("bad_review_percentage", (count("product_id").over(windowSpec) - 1) / count("product_id").over(windowSpec) * 100)

df_ranked_by_rating_count = df_cleaned.withColumn("rank", expr("rank() over (order by rating_count desc)"))

# Combine all DataFrames into a single DataFrame
final_df = df_above_4.union(df_above_4_below_3).union(df_with_bad_review_percentage).union(df_ranked_by_rating_count)

# Repartition the DataFrame to a single partition
final_df_single_partition = final_df.repartition(1)

# Write results to S3 as a single Parquet file
final_df_single_partition.write.parquet(s3_output_path, mode="overwrite")
