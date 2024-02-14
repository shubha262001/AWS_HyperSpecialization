from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, regexp_replace

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define the source and target paths
s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles"
s3_output_path = "s3://amazonsales-capstone-sk/transformed/"

# Read the CSV file into a DataFrame
df = spark.read.csv(s3_input_path, header=True, inferSchema=True)

# Drop unnecessary columns
drop_columns = ["user_id", "user_name", "review_id", "review_title", "review_content", "img_link", "product_link", "about_product"]
df = df.drop(*drop_columns)

# Convert the 'rating' column to numeric type
df = df.withColumn("rating", col("rating").cast("float"))

# Calculate above_4_rating and 3to4_rating at the row level
df = df.withColumn("above_4_rating", when(col("rating") > 4.0, 1).otherwise(0))
df = df.withColumn("3to4_rating", when((col("rating") >= 3.0) & (col("rating") <= 4.0), 1).otherwise(0))

# Split the 'category' column into separate columns
df = df.withColumn("category_levels", split(df["category"], "\\|"))
df = df.select(
    "*",
    col("category_levels")[0].alias("main_category"),
    col("category_levels")[1].alias("sub_category1"),
    col("category_levels")[2].alias("sub_category2"),
    col("category_levels")[3].alias("sub_category3"),
    col("category_levels")[4].alias("sub_category4")
).drop("category", "category_levels")

# Clean the 'discounted_price' and 'actual_price' columns
df = df.withColumn("discounted_price", regexp_replace(col("discounted_price"), "[^0-9]", ""))
df = df.withColumn("actual_price", regexp_replace(col("actual_price"), "[^0-9]", ""))

# Calculate bad review percentage
total_reviews = df.count()
bad_reviews = df.filter((col("rating") >= 1.0) & (col("rating") < 3.0)).count()
bad_review_percentage = (bad_reviews / total_reviews) * 100

# Calculate top performers based on the rating count
top_performers_df = df.groupBy("product_id").agg(
    sum("above_4_rating").alias("above_4_rating_count"),
    sum("3to4_rating").alias("3to4_rating_count"),
    count("rating").alias("rating_count")
).orderBy(col("above_4_rating_count").desc()).limit(10)
top_performers_list = top_performers_df.select("product_id").collect()

# Add a column indicating whether a product is a top performer
df = df.withColumn("top_performer", when(col("product_id").isin(top_performers_list), 1).otherwise(0))

# Extract brand name from 'product_name' column
df = df.withColumn("brand_name", split(col("product_name"), " ")[0])

# Write the final DataFrame to a single Parquet file
df.coalesce(1).write.parquet(s3_output_path, mode="overwrite")
