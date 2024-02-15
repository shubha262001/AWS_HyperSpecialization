#####IMPORT REQUIRED LIBRARIES ######

import os
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, lit, udf, when, split
from pyspark.sql.types import FloatType
from awsglue.job import Job
import csv

######DEFINE FUNCTIONS TO TRANFORM DATA####
############################################
def extract_brand_name(dataframe):
    return dataframe.withColumn("brand_name", split(col("product_name"), " ")[0])

###FUNCTION TO CALCULATE bayesian_average_rating ###

# Define Bayesian average rating function as a UDF
def bayesian_average_rating(overall_rating, num_ratings, prior_rating=3.9, prior_num_ratings=100):
   
    # Calculations 
    bayesian_rating = (prior_num_ratings * prior_rating + num_ratings * overall_rating) / (prior_num_ratings + num_ratings)
    return bayesian_rating

# Convert Python function to UDF
bayesian_average_rating_udf = udf(bayesian_average_rating, FloatType())

# Function to calculate Bayesian average rating for each row in a DataFrame
def calculate_bayesian_average(df):
    # Apply the UDF to create a new column 'bayesian_average_rating'
    df_with_bayesian_average = df.withColumn('bayesian_average_rating',bayesian_average_rating_udf(col('rating'), col('rating_count')))
    return df_with_bayesian_average

####FUNCTION TO CREATE CATEGORIES  ###
def split_column_and_expand(df, column, delimiter='\|'):
    """
    Split a specified column by a delimiter and expand it into separate columns.
   
    Args:
        df (DataFrame): Input DataFrame.
        column (str): Name of the column to split.
        delimiter (str): Delimiter used to split the column (default is '|').
   
    Returns:
        DataFrame: DataFrame with the original column split into separate columns.
    """
    # Split the specified column by the delimiter and expand it into separate columns
    df_split = df.withColumn("temp_categories", split(col(column), delimiter))
   
    # Select individual elements from the array and alias them as separate columns dynamically
    select_cols = [col("temp_categories")[i].alias(f"{column}_layer_{i+1}") for i in range(5)]
    
    # Select the original columns along with the newly created subcategory columns
    all_cols = [*df.columns, *select_cols]
    df_split = df_split.select(*all_cols)
   
    # Drop the original 'category' column
    df_split = df_split.drop(column)
   
    return df_split
####MORE FUNCTIONS###
def select_desired_columns(dataframe):
    return dataframe.select('product_id', 'discounted_price(₹)', 'actual_price(₹)', 'rating', 'rating_count', 'product_name', 'category')

def move_file(input_path, output_path, filename, dataframe):
    # Write transformed data to new folder
    output_file_path = os.path.join(output_path, filename)
    dataframe.write.parquet(output_file_path)
   
    # Remove original file
    os.remove(os.path.join(input_path, filename))

def replace_null_with_zero(data):
    # Replace null values in the rating_count column with zeros
    data_with_zeros = data.fillna(0)
   
    return data_with_zeros

####RECIEVING INPUTS #######
# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define source and target paths
s3_input_path = "s3://amazon-sales-pipeline-bucket-009/cleaned-files/"
s3_output_path = "s3://amazon-sales-pipeline-bucket-009/transformed files/"
s3_backup_path ="s3://backup-vault-bucket-009/transformed files/"

# Get list of files in the input path
input_files = glueContext.spark_session._jvm.org.apache.hadoop.fs.FileSystem.get(
    glueContext._jvm.java.net.URI(s3_input_path), glueContext._jsc.hadoopConfiguration()
).listStatus(glueContext._jvm.org.apache.hadoop.fs.Path(s3_input_path))

###FUNCTIONING PART OF CODE #######

for file_status in input_files:
    input_file_path = file_status.getPath().toString()
   
    # Load data
    data = spark.read.parquet(input_file_path)
   
    # Apply transformations
    data = select_desired_columns(data)
    data = extract_brand_name(data)
    data = replace_null_with_zero(data)
    data = calculate_bayesian_average(data)
    data = split_column_and_expand(data,'category')
    data_transformed = data.repartition(1)  # Repartition into 1 partitions
    
    # Write cleaned data to the output path in Parquet format
    data_transformed.write.parquet(s3_output_path, mode="append")
    data_transformed.write.parquet(s3_backup_path, mode="append")
    # Delete the input file
    file_system = file_status.getPath().getFileSystem(glueContext._jsc.hadoopConfiguration())
    file_system.delete(file_status.getPath(), True)
job.commit()


-------------------------------------------------------------------------------
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, translate, count
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType

# Create a Spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read the Parquet file from the S3 bucket
s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles/"
s3_output_path = "s3://amazonsales-capstone-sk/transformed/"
df_original = spark.read.parquet(s3_input_path)

# Define functions for data cleaning and transformation
def remove_symbols(column):
    return translate(column, "₹,%", "")

def rename_and_clean_columns(df):
    df = df.withColumnRenamed("discounted_price", "discounted_price(₹)") \
           .withColumnRenamed("actual_price", "actual_price(₹)") \
           .withColumn("discounted_price(₹)", remove_symbols(col("discounted_price(₹)"))) \
           .withColumn("actual_price(₹)", remove_symbols(col("actual_price(₹)")))
    df = df.withColumn("discounted_price(₹)", df["discounted_price(₹)"].cast(IntegerType())) \
           .withColumn("actual_price(₹)", df["actual_price(₹)"].cast(IntegerType()))
    return df

def clean_discount_percentage(df):
    df = df.withColumn("discount_percentage", remove_symbols(col("discount_percentage"))) \
           .withColumn("discount_percentage", col("discount_percentage").cast(FloatType()))
    return df

def replace_null_values(df):
    df = df.fillna("N.A.")
    return df

def drop_duplicate_rows(df):
    df = df.dropDuplicates(["product_id", "discounted_price(₹)", "actual_price(₹)", "rating", "rating_count"])
    return df

def change_data_formats(df):
    df = df.withColumn("rating", col("rating").cast(FloatType())) \
           .withColumn("rating_count", remove_symbols(col("rating_count"))) \
           .withColumn("rating_count", col("rating_count").cast(IntegerType()))
    return df

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
    
    # Drop specified columns
    drop_columns = ['user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'about_product']
    df = df.drop(*drop_columns)
    
    return df

# Check if all required columns are present
required_columns = ['product_id', 'discounted_price', 'actual_price', 'rating', 'rating_count', 'category']
missing_columns = set(required_columns) - set(df_original.columns)
if missing_columns:
    raise ValueError(f"Missing columns: {missing_columns}")

# Apply all transformations
df_cleaned = apply_business_logic(df_original)

# Write the cleaned data to the target S3 folder as a single Parquet file
df_cleaned.coalesce(1).write.parquet(s3_output_path, mode="overwrite")


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, translate, count
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType

# Create a Spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read the Parquet file from the S3 bucket
s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles/"
s3_output_path = "s3://amazonsales-capstone-sk/transformed/"
df_original = spark.read.parquet(s3_input_path)

# Define functions for data cleaning and transformation
def remove_symbols(column):
    return translate(column, "₹,%", "")

def rename_and_clean_columns(df):
    df = df.withColumnRenamed("discounted_price", "discounted_price(₹)") \
           .withColumnRenamed("actual_price", "actual_price(₹)") \
           .withColumn("discounted_price(₹)", remove_symbols(col("discounted_price(₹)"))) \
           .withColumn("actual_price(₹)", remove_symbols(col("actual_price(₹)")))
    df = df.withColumn("discounted_price(₹)", df["discounted_price(₹)"].cast(IntegerType())) \
           .withColumn("actual_price(₹)", df["actual_price(₹)"].cast(IntegerType()))
    return df

def clean_discount_percentage(df):
    df = df.withColumn("discount_percentage", remove_symbols(col("discount_percentage"))) \
           .withColumn("discount_percentage", col("discount_percentage").cast(FloatType()))
    return df

def replace_null_values(df):
    df = df.fillna("N.A.")
    return df

def drop_duplicate_rows(df):
    df = df.dropDuplicates(["product_id", "discounted_price(₹)", "actual_price(₹)", "rating", "rating_count"])
    return df

def change_data_formats(df):
    df = df.withColumn("rating", col("rating").cast(FloatType())) \
           .withColumn("rating_count", remove_symbols(col("rating_count"))) \
           .withColumn("rating_count", col("rating_count").cast(IntegerType()))
    return df

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

# Apply all transformations
df_cleaned = apply_business_logic(df_original)

# Write the cleaned data to the target S3 folder as a single Parquet file
df_cleaned.coalesce(1).write.parquet(s3_output_path, mode="overwrite")
