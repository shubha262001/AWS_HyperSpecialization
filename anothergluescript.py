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

def extract_brand_name(dataframe):
    return dataframe.withColumn("brand_name", split(col("product_name"), " ")[0])
###-----------------------------------------###
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
###-----------------------------------------------###

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
####################################################################################
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

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define source and target paths
s3_input_path = "s3://data-pipeline-bucket-009/cleaned-files/"
s3_output_path = "s3://data-pipeline-bucket-009/transformed files/"

# Get list of files in the input path
input_files = glueContext.spark_session._jvm.org.apache.hadoop.fs.FileSystem.get(
    glueContext._jvm.java.net.URI(s3_input_path), glueContext._jsc.hadoopConfiguration()
).listStatus(glueContext._jvm.org.apache.hadoop.fs.Path(s3_input_path))

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
job.commit()
