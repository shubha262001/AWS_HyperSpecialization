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
