from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, translate, expr, count, avg, desc, when
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.window import Window

# Create a SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define the source and target paths
s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles"
s3_output_path = "s3://amazonsales-capstone-sk/transformed/"

# Read the CSV file into a DataFrame
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database="amazonsales-sk-capstone", table_name="cleanedfiles", transformation_ctx="dynamic_frame")
df_original = dynamic_frame.toDF()

# Function to remove symbols from column values
def remove_symbols(column):
    return translate(column, "₹,%", "")

# Function to change column names and remove symbols
def rename_and_clean_columns(df):
    df = df.drop('user_id', 'user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link')\
           .withColumnRenamed("discounted_price", "discounted_price(₹)") \
           .withColumnRenamed("actual_price", "actual_price(₹)") \
           .withColumn("discounted_price(₹)", remove_symbols(col("discounted_price(₹)"))) \
           .withColumn("actual_price(₹)", remove_symbols(col("actual_price(₹)")))
    df = df.withColumn("discounted_price(₹)", df["discounted_price(₹)"].cast(IntegerType())) \
           .withColumn("actual_price(₹)", df["actual_price(₹)"].cast(IntegerType()))
    return df

# Function to split category column into full names
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

# Apply basic data cleaning and transformation operations
df_cleaned = df_original
df_cleaned = rename_and_clean_columns(df_cleaned)
df_cleaned = split_category(df_cleaned)
df_cleaned = clean_discount_percentage(df_cleaned)
df_cleaned = replace_null_values(df_cleaned)

# Calculate top performers based on the rating count
window_spec = Window.partitionBy("product_id").orderBy(desc("rating_count"))
df_cleaned = df_cleaned.withColumn("rank", when(col("rating_count") > 4, 1).otherwise(0)) \
                       .withColumn("top_performer", when(col("rank") == 1, 1).otherwise(0)) \
                       .drop("rank")

# Write results to S3 as a single Parquet file
df_cleaned.coalesce(1).write.parquet(s3_output_path, mode="overwrite")
