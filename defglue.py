from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, regexp_replace, lit, count, sum
from pyspark.sql.types import FloatType

def load_data(spark, s3_input_path):
    return spark.read.parquet(s3_input_path)

def clean_data(df):
    drop_columns = ["user_id", "user_name", "review_id", "review_title", "review_content", "img_link", "product_link", "about_product"]
    df = df.drop(*drop_columns)
    
    df = df.withColumn("rating", df["rating"].cast("float"))
    df = df.withColumn("rating", when(col("rating").isNull(), 0).otherwise(col("rating")))
    
    df = df.withColumn("above_4_rating", when(col("rating") > 4.0, 1).otherwise(0)) \
           .withColumn("3to4_rating", when((col("rating") >= 3.0) & (col("rating") <= 4.0), 1).otherwise(0))
    
    df = df.withColumn("category_levels", split(df["category"], "\\|"))
    df = df.select(
        "*",
        col("category_levels")[0].alias("main_category"),
        col("category_levels")[1].alias("sub_category1"),
        col("category_levels")[2].alias("sub_category2"),
        col("category_levels")[3].alias("sub_category3"),
        col("category_levels")[4].alias("sub_category4")
    ).drop("category", "category_levels")
    
    df = df.withColumn("discounted_price", regexp_replace(col("discounted_price"), "[^0-9]", "")) \
           .withColumn("actual_price", regexp_replace(col("actual_price"), "[^0-9]", ""))
    
    return df

def calculate_metrics(df):
    total_reviews = df.count()
    bad_reviews_count = df.filter((col("rating") >= 1.0) & (col("rating") < 3.0)).count()
    bad_review_percentage = (bad_reviews_count / total_reviews) * 100
    
    df = df.withColumn("bad_review_percentage", lit(bad_review_percentage))
    
    return df

def find_top_performers(df):
    top_performers_df = df.groupBy("product_id").agg(
        sum("above_4_rating").alias("above_4_rating_count"),
        sum("3to4_rating").alias("3to4_rating_count"),
        count("rating").alias("rating_count")
    ).orderBy(col("above_4_rating_count").desc()).limit(10)
    
    top_performers_list = top_performers_df.select("product_id").collect()
    df = df.withColumn("top_performer", when(col("product_id").isin(top_performers_list), 1).otherwise(0))
    
    return df

def write_output(df, s3_output_path):
    df.coalesce(1).write.parquet(s3_output_path, mode="overwrite")

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define the source and target paths
s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles"
s3_output_path = "s3://amazonsales-capstone-sk/transformed/"

# Load the data
df = load_data(spark, s3_input_path)

# Clean the data
df = clean_data(df)

# Calculate metrics
df = calculate_metrics(df)

# Find top performers
df = find_top_performers(df)

# Write the output
write_output(df, s3_output_path)

