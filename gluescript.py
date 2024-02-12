from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Data_Transformation").getOrCreate()

# Read data from S3
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database="amazonsales-sk-capstone", table_name="cleanedfiles", transformation_ctx="dynamic_frame")

# Convert DynamicFrame to DataFrame
df = dynamic_frame.toDF()

# Define the output path for the single Parquet file
s3_output_path_final = "s3://amazonsales-capstone-sk/transformed/single_file/"

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

# Apply business logic transformations
df = apply_business_logic(df)

# Repartition the DataFrame to a single partition
df_single_partition = df.repartition(1)

# Write results to S3 as a single Parquet file
df_single_partition.write.parquet(s3_output_path_final, mode="overwrite")
