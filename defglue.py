from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, sum, desc
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
df = df.withColumn("category_levels", col("category").split("\\|"))
df = df.select(
    "*",
    col("category_levels")[0].alias("main_category"),
    col("category_levels")[1].alias("sub_category1"),
    col("category_levels")[2].alias("sub_category2"),
    col("category_levels")[3].alias("sub_category3"),
    col("category_levels")[4].alias("sub_category4")
).drop("category", "category_levels")

# Convert the 'rating' column to numeric type
df = df.withColumn("rating", df["rating"].cast(FloatType()))

# Calculate above_4_rating and 3to4_rating at the row level
df = df.withColumn("above_4_rating", when(col("rating") > 4.0, 1).otherwise(0))
df = df.withColumn("3to4_rating", when((col("rating") >= 3.0) & (col("rating") <= 4.0), 1).otherwise(0))

# Aggregate to get the total rating count for each product
aggregated_df = df.groupBy("product_id").agg(
    avg("rating").cast(FloatType()).alias("average_rating"),
    sum("above_4_rating").alias("above_4_rating_count"),
    sum("3to4_rating").alias("3to4_rating_count"),
    sum("rating_count").alias("total_rating_count")
)

# Calculate bad_review_percentage
bad_review_percentage_df = aggregated_df.withColumn("bad_review_percentage", 
                                                    (sum(when(col("rating") < 3.0, 1).otherwise(0)) / col("total_rating_count")) * 100)

# Extract brand from product_name column
df = df.withColumn("brand", col("product_name").substr(0, col("product_name").instr(" ")))

# Identify top performers
top_performers_df = aggregated_df.orderBy(desc("above_4_rating_count")).limit(10)
top_performers_list = top_performers_df.select("product_id").collect()

# Add a column indicating whether a product is a top performer
df = df.withColumn("top_performer", when(col("product_id").isin(top_performers_list), 1).otherwise(0))

# Join the aggregated data back to the original DataFrame
final_df = df.join(aggregated_df, "product_id").drop("total_rating_count")

# Write the final DataFrame to a single Parquet file
final_df.coalesce(1).write.parquet(s3_output_path, mode="overwrite")

