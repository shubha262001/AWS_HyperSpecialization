from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, when, avg, sum, desc
from pyspark.sql.types import FloatType

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define the source and target paths
s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles"
s3_output_path = "s3://amazonsales-capstone-sk/transformed/"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(s3_input_path)

# Drop unnecessary columns
drop_columns = ["user_id", "user_name", "review_id", "review_title", "review_content", "img_link", "product_link"]
df = df.drop(*drop_columns)

# Split the 'category' column into separate columns
split_categories = split(df['category'], '\\|')
df = df.withColumn("main_category", split_categories.getItem(0))
df = df.withColumn("sub_category1", split_categories.getItem(1))
df = df.withColumn("sub_category2", split_categories.getItem(2))
df = df.withColumn("sub_category3", split_categories.getItem(3))
df = df.withColumn("sub_category4", split_categories.getItem(4))
df = df.drop("category")

# Convert the 'rating' column to numeric type
df = df.withColumn("rating", df["rating"].cast(FloatType()))

# Calculate above_4_rating and 3to4_rating at the row level
df = df.withColumn("above_4_rating", when(col("rating") > 4.0, 1).otherwise(0))
df = df.withColumn("3to4_rating", when((col("rating") >= 3.0) & (col("rating") <= 4.0), 1).otherwise(0))

# Aggregate to get the total rating count for each product
aggregated_df = df.groupBy("product_id").agg(
    avg("rating").alias("average_rating"),
    sum("above_4_rating").alias("above_4_rating_count"),
    sum("3to4_rating").alias("3to4_rating_count"),
    sum("rating_count").alias("total_rating_count")
)

# Calculate bad_review_percentage
aggregated_df = aggregated_df.withColumn("bad_review_percentage", (sum(when(col("rating") < 3.0, 1).otherwise(0)) / sum("total_rating_count")) * 100)

# Calculate top performers based on the rating count
top_performers_df = aggregated_df.orderBy(desc("above_4_rating_count")).limit(10)
top_performers_list = top_performers_df.select("product_id").collect()

# Add a column indicating whether a product is a top performer
df = df.withColumn("top_performer", when(col("product_id").isin(top_performers_list), 1).otherwise(0))

# Join the aggregated data back to the original DataFrame
final_df = df.join(aggregated_df, "product_id").drop("total_rating_count")

# Write the final DataFrame to a single Parquet file
final_df.coalesce(1).write.parquet(s3_output_path, mode="overwrite")
