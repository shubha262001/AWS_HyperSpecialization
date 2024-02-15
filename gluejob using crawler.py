import os
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, lit, udf, when
from pyspark.sql.types import FloatType
from awsglue.job import Job

def extract_brand_name(dataframe):
    return dataframe.withColumn("brand_name", split(col("product_name"), " ")[0])

def split_column_and_expand(df, column, delimiter='\|'):
    df_split = df.withColumn("temp_categories", split(col(column), delimiter))
    select_cols = [col("temp_categories")[i].alias(f"{column}_layer_{i+1}") for i in range(5)]
    all_cols = [*df.columns, *select_cols]
    df_split = df_split.select(*all_cols)
    df_split = df_split.drop(column)
    return df_split

def select_desired_columns(dataframe):
    return dataframe.select('product_id', 'discounted_price(₹)', 'actual_price(₹)', 'rating', 'rating_count', 'product_name', 'category')

def replace_null_with_zero(data):
    data_with_zeros = data.fillna(0)
    return data_with_zeros

def calculate_above_4_rating(rating):
    return when(rating > 4, 1).otherwise(0)

def calculate_3_to_4_rating(rating):
    return when((rating >= 3) & (rating <= 4), 1).otherwise(0)

def calculate_bad_review_percentage(rating_count):
    return ((rating_count - 1) / rating_count) * 100

def calculate_top_performers(df):
    top_performers_list = df.groupBy("product_id").count().orderBy(col("count").desc()).limit(10).select("product_id")
    return df.withColumn("top_performer", col("product_id").isin(top_performers_list).cast(FloatType()))

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles/"
s3_output_path = "s3://amazonsales-capstone-sk/transformed/"

input_files = glueContext.spark_session._jvm.org.apache.hadoop.fs.FileSystem.get(
    glueContext._jvm.java.net.URI(s3_input_path), glueContext._jsc.hadoopConfiguration()
).listStatus(glueContext._jvm.org.apache.hadoop.fs.Path(s3_input_path))

for file_status in input_files:
    input_file_path = file_status.getPath().toString()
    data = spark.read.parquet(input_file_path)
    data = select_desired_columns(data)
    data = extract_brand_name(data)
    data = replace_null_with_zero(data)
    data = split_column_and_expand(data,'category')
    data = data.withColumn('above_rating_4', calculate_above_4_rating(col('rating')))
    data = data.withColumn('3_to_4_rating', calculate_3_to_4_rating(col('rating')))
    data = data.withColumn('bad_review_percentage', lit(calculate_bad_review_percentage(col('rating_count'))).cast(FloatType()))
    data = calculate_top_performers(data)
    data = data.drop('user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'user_id', 'about_product')
    data_transformed = data.coalesce(1)
    data_transformed.write.parquet(s3_output_path)
    #file_system = file_status.getPath().getFileSystem(glueContext._jsc.hadoopConfiguration())
    #file_system.delete(file_status.getPath(), True)

job.commit()




============================
import os
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, lit, udf, when
from pyspark.sql.types import FloatType
from awsglue.job import Job

def extract_brand_name(dataframe):
    return dataframe.withColumn("brand_name", split(col("product_name"), " ")[0])

def split_column_and_expand(df, column, delimiter='\|'):
    df_split = df.withColumn("temp_categories", split(col(column), delimiter))
    select_cols = [col("temp_categories")[i].alias(f"{column}_layer_{i+1}") for i in range(5)]
    all_cols = [*df.columns, *select_cols]
    df_split = df_split.select(*all_cols)
    df_split = df_split.drop(column)
    return df_split

def select_desired_columns(dataframe):
    return dataframe.select('product_id', 'discounted_price(₹)', 'actual_price(₹)', 'rating', 'rating_count', 'product_name', 'category')

def move_file(input_path, output_path, filename, dataframe):
    output_file_path = os.path.join(output_path, filename)
    dataframe.write.parquet(output_file_path)
    os.remove(os.path.join(input_path, filename))

def replace_null_with_zero(data):
    data_with_zeros = data.fillna(0)
    return data_with_zeros

def calculate_above_4_rating(rating):
    return when(rating > 4, 1).otherwise(0)

def calculate_3_to_4_rating(rating):
    return when((rating >= 3) & (rating <= 4), 1).otherwise(0)

def calculate_bad_review_percentage(rating_count):
    return ((rating_count - 1) / rating_count) * 100

def calculate_top_performers(df):
    top_performers_list = df.groupBy("product_id").count().orderBy(col("count").desc()).limit(10).select("product_id")
    return df.withColumn("top_performer", col("product_id").isin(top_performers_list).cast(FloatType()))

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles/"
s3_output_path = "s3://amazonsales-capstone-sk/transformed/"

input_files = glueContext.spark_session._jvm.org.apache.hadoop.fs.FileSystem.get(
    glueContext._jvm.java.net.URI(s3_input_path), glueContext._jsc.hadoopConfiguration()
).listStatus(glueContext._jvm.org.apache.hadoop.fs.Path(s3_input_path))

for file_status in input_files:
    input_file_path = file_status.getPath().toString()
    data = spark.read.parquet(input_file_path)
    data = select_desired_columns(data)
    data = extract_brand_name(data)
    data = replace_null_with_zero(data)
    data = split_column_and_expand(data,'category')
    data = data.withColumn('above_rating_4', calculate_above_4_rating(col('rating')))
    data = data.withColumn('3_to_4_rating', calculate_3_to_4_rating(col('rating')))
    data = data.withColumn('bad_review_percentage', lit(calculate_bad_review_percentage(col('rating_count'))).cast(FloatType()))
    data = calculate_top_performers(data)
    data = data.drop('user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'user_id', 'about_product')
    data_transformed = data.coalesce(1)
    data_transformed.write.mode("overwrite").parquet(s3_output_path)

job.commit()

===============================
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, translate, split, substring, desc, count
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType

# Create a SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read data from Glue catalog
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database="amazonsales-sk-capstone", table_name="cleanedfiles", transformation_ctx="dynamic_frame")
df_original = dynamic_frame.toDF()

# Function to change column names and remove symbols
def rename_and_clean_columns(df):
    df = df.withColumn("discounted_price(₹)", translate(col("discounted_price"), "₹,%", "").cast(FloatType())) \
           .withColumn("actual_price(₹)", translate(col("actual_price"), "₹,%", "").cast(FloatType()))
    return df

# Function to clean discount_percentage column
def clean_discount_percentage(df):
    df = df.withColumn("discount_percentage", translate(col("discount_percentage"), "%", "").cast(FloatType()))
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

# Split the 'category' column into separate columns
df_cleaned = df_cleaned.withColumn("category_levels", split("category", "\|"))
df_cleaned = df_cleaned.select(
    "*",
    col("category_levels")[0].alias("main_category"),
    col("category_levels")[1].alias("sub_category1"),
    col("category_levels")[2].alias("sub_category2"),
    col("category_levels")[3].alias("sub_category3"),
    col("category_levels")[4].alias("sub_category4")
).drop("category", "category_levels")

# Calculate the average rating, above_4_rating, and 3to4_rating
df_cleaned = df_cleaned.withColumn("above_4_rating", (col("rating") > 4.0).cast(IntegerType()))
df_cleaned = df_cleaned.withColumn("3to4_rating", ((col("rating") >= 3.0) & (col("rating") <= 4.0)).cast(IntegerType()))

# Calculate bad_review_percentage
windowSpec = Window.partitionBy("product_id")
df_cleaned = df_cleaned.withColumn("bad_review_percentage", ((count("product_id").over(windowSpec) - 1) / count("product_id").over(windowSpec) * 100).cast(FloatType()))

# Calculate top performers
top_performers_list = df_cleaned.groupBy("product_id").count().orderBy(desc("count")).limit(10).select("product_id").collect()
top_performers_list = [row.product_id for row in top_performers_list]
df_cleaned = df_cleaned.withColumn("top_performer", col("product_id").isin(top_performers_list).cast(IntegerType()))

# Calculate brand
df_cleaned = df_cleaned.withColumn("brandname", substring("product_name", 1, 4))

# Write results to S3 as a single Parquet file
s3_output_path = "s3://amazonsales-capstone-sk/transformed/"
df_cleaned.write.parquet(s3_output_path, mode="overwrite")


=====================
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, translate, expr, count, desc, split, substring
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType

# Create a SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Define source and target paths
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

# Split the 'category' column into separate columns
df_cleaned = df_cleaned.withColumn("category_levels", split("category", "\|"))
df_cleaned = df_cleaned.select(
    "*",
    col("category_levels")[0].alias("main_category"),
    col("category_levels")[1].alias("sub_category1"),
    col("category_levels")[2].alias("sub_category2"),
    col("category_levels")[3].alias("sub_category3"),
    col("category_levels")[4].alias("sub_category4")
).drop("category", "category_levels")

# Calculate the average rating, above_4_rating, and 3to4_rating
df_cleaned = df_cleaned.withColumn("above_4_rating", ((col("rating") > 4.0).cast(IntegerType())))
df_cleaned = df_cleaned.withColumn("3to4_rating", (((col("rating") >= 3.0) & (col("rating") <= 4.0)).cast(IntegerType())))

# Calculate bad_review_percentage
windowSpec = Window.partitionBy("product_id")
df_cleaned = df_cleaned.withColumn("bad_review_percentage", ((count("product_id").over(windowSpec) - 1) / count("product_id").over(windowSpec) * 100).cast(FloatType()))

# Calculate top performers
top_performers_list = df_cleaned.groupBy("product_id").count().orderBy(desc("count")).limit(10).select("product_id").collect()
top_performers_list = [row.product_id for row in top_performers_list]
df_cleaned = df_cleaned.withColumn("top_performer", col("product_id").isin(top_performers_list).cast(IntegerType()))

# Calculate brand
df_cleaned = df_cleaned.withColumn("brandname", substring("product_name", 1, 4))

# Repartition the DataFrame to a single partition
final_df_single_partition = df_cleaned.coalesce(1)

# Write results to S3 as a single Parquet file
final_df_single_partition.write.parquet(s3_output_path, mode="overwrite")


=========================================

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, translate, expr, count, desc, split, substring, round
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType

# Create a SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Read data from S3
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database="amazonsales-sk-capstone", table_name="cleanedfiles", transformation_ctx="dynamic_frame")

# Convert DynamicFrame to DataFrame
df_original = dynamic_frame.toDF()

# Function to remove symbols from column values
def remove_symbols(column):
    return translate(column, "₹,%", "")

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
    df = df.dropDuplicates(["product_id", "discounted_price", "actual_price", "rating", "rating_count"])
    return df

# Function to change data formats
def change_data_formats(df):
    df = df.withColumn("rating", round(col("rating"), 1).cast(FloatType())) \
           .withColumn("rating_count", remove_symbols(col("rating_count"))) \
           .withColumn("rating_count", col("rating_count").cast(IntegerType()))
    return df

# Apply all business logic transformations
def apply_business_logic(df):
    df = clean_discount_percentage(df)
    df = replace_null_values(df)
    df = drop_duplicate_rows(df)
    df = change_data_formats(df)
    return df

# Apply basic data cleaning and transformation operations
df_cleaned = apply_business_logic(df_original)

# Split the 'category' column into separate columns
df_cleaned = df_cleaned.withColumn("category_levels", split("category", "\|"))
df_cleaned = df_cleaned.select(
    "*",
    col("category_levels")[0].alias("main_category"),
    col("category_levels")[1].alias("sub_category1"),
    col("category_levels")[2].alias("sub_category2"),
    col("category_levels")[3].alias("sub_category3"),
    col("category_levels")[4].alias("sub_category4")
).drop("category", "category_levels")

# Calculate the average rating, above_4_rating, and 3to4_rating
df_cleaned = df_cleaned.withColumn("above_4_rating", ((col("rating") > 4.0).cast(IntegerType())))
df_cleaned = df_cleaned.withColumn("3to4_rating", (((col("rating") >= 3.0) & (col("rating") <= 4.0)).cast(IntegerType())))

# Calculate bad_review_percentage
windowSpec = Window.partitionBy("product_id")
df_cleaned = df_cleaned.withColumn("bad_review_percentage", (((count("product_id").over(windowSpec) - 1) / count("product_id").over(windowSpec)) * 100).cast(FloatType()))

# Calculate top performers
top_performers_list = df_cleaned.groupBy("product_id").count().orderBy(desc("count")).limit(10).select("product_id").collect()
top_performers_list = [row.product_id for row in top_performers_list]
df_cleaned = df_cleaned.withColumn("top_performer", col("product_id").isin(top_performers_list).cast(IntegerType()))

# Calculate brand
df_cleaned = df_cleaned.withColumn("brandname", expr("substring_index(product_name, ' ', 1)"))

# Repartition the DataFrame to a single partition
final_df_single_partition = df_cleaned.coalesce(1)

# Drop unnecessary columns
drop_columns_final = ['user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link']
final_df_single_partition = final_df_single_partition.drop(*drop_columns_final)

# Write results to S3 as a single Parquet file
final_df_single_partition.write.parquet("s3://amazonsales-capstone-sk/transformedfiles/", mode="overwrite")


======================================
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, translate, expr, count, desc, split, substring, round
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Define source and target paths
s3_output_path = "s3://amazonsales-capstone-sk/transformedfiles/"

# Read data from Glue catalog using the specified crawler
db_name = "amazonsales-sk-capstone"
table_name = "cleanedfiles"
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=table_name, transformation_ctx="dynamic_frame")

# Convert DynamicFrame to DataFrame
df_original = dynamic_frame.toDF()

# Function to remove symbols from column values
def remove_symbols(column):
    return translate(column, "₹,%", "")

# Function to clean discount_percentage column
def clean_discount_percentage(df):
    if "discount_percentage" in df.columns:
        df = df.withColumn("discount_percentage", remove_symbols(col("discount_percentage"))) \
               .withColumn("discount_percentage", col("discount_percentage").cast(FloatType()))
    return df

# Function to replace null values with 'N.A.'
def replace_null_values(df):
    df = df.fillna("N.A.")
    return df

# Function to drop duplicate rows based on specified columns
def drop_duplicate_rows(df):
    df = df.dropDuplicates(["product_id", "discounted_price", "actual_price", "rating", "rating_count"])
    return df

# Function to change data formats
def change_data_formats(df):
    df = df.withColumn("rating", round(col("rating"), 1).cast(FloatType())) \
           .withColumn("rating_count", remove_symbols(col("rating_count"))) \
           .withColumn("rating_count", col("rating_count").cast(IntegerType()))
    return df

# Apply all business logic transformations
def apply_business_logic(df):
    df = clean_discount_percentage(df)
    df = replace_null_values(df)
    df = drop_duplicate_rows(df)
    df = change_data_formats(df)
    return df

# Apply basic data cleaning and transformation operations
df_cleaned = apply_business_logic(df_original)

# Split the 'category' column into separate columns
df_cleaned = df_cleaned.withColumn("category_levels", split("category", "\|"))
df_cleaned = df_cleaned.select(
    "*",
    col("category_levels")[0].alias("main_category"),
    col("category_levels")[1].alias("sub_category1"),
    col("category_levels")[2].alias("sub_category2"),
    col("category_levels")[3].alias("sub_category3"),
    col("category_levels")[4].alias("sub_category4")
).drop("category", "category_levels")

# Calculate the average rating, above_4_rating, and 3to4_rating
df_cleaned = df_cleaned.withColumn("above_4_rating", ((col("rating") > 4.0).cast(IntegerType())))
df_cleaned = df_cleaned.withColumn("3to4_rating", (((col("rating") >= 3.0) & (col("rating") <= 4.0)).cast(IntegerType())))

# Calculate bad_review_percentage
windowSpec = Window.partitionBy("product_id")
df_cleaned = df_cleaned.withColumn("bad_review_percentage", (((count("product_id").over(windowSpec) - 1) / count("product_id").over(windowSpec)) * 100).cast(FloatType()))

# Calculate top performers
top_performers_list = df_cleaned.groupBy("product_id").count().orderBy(desc("count")).limit(10).select("product_id").collect()
top_performers_list = [row.product_id for row in top_performers_list]
df_cleaned = df_cleaned.withColumn("top_performer", col("product_id").isin(top_performers_list).cast(IntegerType()))

# Calculate brand
df_cleaned = df_cleaned.withColumn("brandname", expr("substring_index(product_name, ' ', 1)"))

# Repartition the DataFrame to a single partition
final_df_single_partition = df_cleaned.coalesce(1)

# Write results to S3 as a single Parquet file
final_df_single_partition.write.parquet(s3_output_path, mode="overwrite")
