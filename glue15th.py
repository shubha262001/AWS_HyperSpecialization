{
  "product_id": "B07JW9H4J1",
  "discounted_price": 399,
  "actual_price": 1099,
  "rating": "4.2",
  "rating_count": "24,269",
  "brand_name": "Wayona",
  "discount_percentage(%)": 64,
  "above_4_rating": 0,
  "3to4_rating": 1,
  "bad_review": 0,
  "top_performer": 1
}



----------------------------------------------
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import split, col, when, udf
from pyspark.sql.types import FloatType, StringType, IntegerType, DoubleType
from awsglue.job import Job

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Load the data into a DataFrame
df = spark.read.parquet("s3://amazonsales-capstone-sk/cleanedfiles/")

# Handle null values by replacing them with zeros
df = df.na.fill(0)

# Extract brand name from the first word of the product_name
df = df.withColumn("brand_name", split(col("product_name"), " ")[0])

# Convert discount_percentage to double and remove % symbol
df = df.withColumn("discount_percentage(%)", col("discount_percentage").substr(1, 2).cast(DoubleType()))

# Convert rating to double and handle cases where it is empty or not a valid number
def parse_rating(rating):
    try:
        return float(rating)
    except ValueError:
        return None

parse_rating_udf = udf(parse_rating, DoubleType())

df = df.withColumn("rating_value", parse_rating_udf(col("rating")))

# Calculate above_4_rating, 3to4_rating, and bad_review_percentage
df = df.withColumn("above_4_rating", when(col("rating") > 4, 1).otherwise(0)) \
    .withColumn("3to4_rating", when((col("rating") >= 3) & (col("rating") <= 4), 1).otherwise(0)) \
    .withColumn("bad_review", when((col("above_4_rating") + col("3to4_rating")) == 0, 1).otherwise(0)) \
    .withColumn("bad_review_percentage", ((col("bad_review") / col("rating_count").cast(IntegerType())) * 100).cast(FloatType()))

# Calculate top performers
from pyspark.sql.functions import desc
top_performers_list = df.groupBy("product_id").count().orderBy(desc("count")).limit(10).select("product_id").collect()
top_performers_list = [row.product_id for row in top_performers_list]
df = df.withColumn("top_performer", col("product_id").isin(top_performers_list).cast(IntegerType()))

# Drop unnecessary columns
drop_columns = ['rating_value','discount_percentage','user_id','category','product_name','user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'about_product']
df_final = df.drop(*drop_columns)

# Write the transformed data back to S3
df_final.write.mode("overwrite").parquet("s3://amazonsales-capstone-sk/transformed/")

job.commit()





------------------------------------------
# Calculate above_4_rating, 3to4_rating, and bad_review_percentage
df = df.withColumn("above_4_rating", when(col("rating") > 4, 1).otherwise(0)) \
    .withColumn("3to4_rating", when((col("rating") >= 3) & (col("rating") <= 4), 1).otherwise(0)) \
    .withColumn("bad_review", when((col("above_4_rating") + col("3to4_rating")) == 0, 1).otherwise(0)) \
    .withColumn("bad_review_percentage", ((col("bad_review") / col("rating_count").cast(IntegerType())) * 100).cast(FloatType()))

"rating_value": 4.2,
"discount_percentage": "64%",





=============================================================
{
  "product_id": "B07JW9H4J1",
  "discounted_price": 399,
  "actual_price": 1099,
  "discount_percentage": "64%",
  "rating": "4.2",
  "rating_count": "24,269",
  "brand_name": "Wayona",
  "discount_percentage(%)": 64,
  "rating_value": 4.2,
  "above_4_rating": 1,
  "3to4_rating": 0,
  "bad_review": 0,
  "bad_review_percentage": 0,
  "top_performer": 1
}

----------------------------------------------------------------------------------
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import split, col, when, udf
from pyspark.sql.types import FloatType, StringType, IntegerType, DoubleType
from awsglue.job import Job

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Load the data into a DataFrame
df = spark.read.parquet("s3://amazonsales-capstone-sk/cleanedfiles/")

# Extract brand name from the first word of the product_name
df = df.withColumn("brand_name", split(col("product_name"), " ")[0])

# Convert discount_percentage to double and remove % symbol
df = df.withColumn("discount_percentage(%)", col("discount_percentage").substr(1, 2).cast(DoubleType()))

# Convert rating to double and handle cases where it is empty or not a valid number
def parse_rating(rating):
    try:
        return float(rating)
    except ValueError:
        return None

parse_rating_udf = udf(parse_rating, DoubleType())

df = df.withColumn("rating_value", parse_rating_udf(col("rating")))

# Calculate above_4_rating, 3to4_rating, and bad_review_percentage
df = df.withColumn("above_4_rating", when(col("rating_value") > 4, 1).otherwise(0)) \
    .withColumn("3to4_rating", when((col("rating_value") >= 3) & (col("rating_value") <= 4), 1).otherwise(0)) \
    .withColumn("bad_review", when((col("above_4_rating") + col("3to4_rating")) == 0, 1).otherwise(0)) \
    .withColumn("bad_review_percentage", (1 - (col("above_4_rating") + col("3to4_rating")))*100)

# Calculate top performers
from pyspark.sql.functions import desc
top_performers_list = df.groupBy("product_id").count().orderBy(desc("count")).limit(10).select("product_id").collect()
top_performers_list = [row.product_id for row in top_performers_list]
df = df.withColumn("top_performer", col("product_id").isin(top_performers_list).cast(IntegerType()))

# Drop unnecessary columns
drop_columns = ['user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'about_product']
df_final = df.drop(*drop_columns)

# Write the transformed data back to S3
df_final.write.mode("overwrite").parquet("s3://amazonsales-capstone-sk/transformed/")

job.commit()
















=======================================
Error Category: UNSUPPORTED_OPERATION_ERROR; An error occurred while calling o206.parquet. org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException

------------------------------------------------------
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import split, col, when, udf
from pyspark.sql.types import FloatType, StringType, IntegerType, StructType, StructField
from pyspark.sql import SparkSession
from awsglue.job import Job

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Define the schema for the DataFrame
schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("discounted_price", FloatType(), True),
    StructField("actual_price", FloatType(), True),
    StructField("discount_percentage", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("rating_count", StringType(), True),
    StructField("about_product", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("review_id", StringType(), True),
    StructField("review_title", StringType(), True),
    StructField("review_content", StringType(), True),
    StructField("img_link", StringType(), True),
    StructField("product_link", StringType(), True)
])

# Load the data into a DataFrame
df = spark.read.format("parquet").schema(schema).load("s3://amazonsales-capstone-sk/cleanedfiles/")

# Extract brand name from the first word of the product_name
df = df.withColumn("brand_name", split(col("product_name"), " ")[0])

# Convert discount_percentage to double and remove % symbol
df = df.withColumn("discount_percentage(%)", col("discount_percentage").substr(1, 2).cast(DoubleType()))

# Convert rating to double and handle cases where it is empty or not a valid number
def parse_rating(rating):
    try:
        return float(rating)
    except ValueError:
        return None

parse_rating_udf = udf(parse_rating, DoubleType())

df = df.withColumn("rating_value", parse_rating_udf(col("rating")))

# Calculate above_4_rating, 3to4_rating, and bad_review_percentage
df = df.withColumn("above_4_rating", when(col("rating_value") > 4, 1).otherwise(0)) \
    .withColumn("3to4_rating", when((col("rating_value") >= 3) & (col("rating_value") <= 4), 1).otherwise(0)) \
    .withColumn("bad_review", when((col("above_4_rating") + col("3to4_rating")) == 0, 1).otherwise(0)) \
    .withColumn("bad_review_percentage", (1 - (col("above_4_rating") + col("3to4_rating")))*100)

# Calculate top performers
from pyspark.sql.functions import desc
top_performers_list = df.groupBy("product_id").count().orderBy(desc("count")).limit(10).select("product_id").collect()
top_performers_list = [row.product_id for row in top_performers_list]
df = df.withColumn("top_performer", col("product_id").isin(top_performers_list).cast(IntegerType()))

# Drop unnecessary columns
drop_columns = ['user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'about_product']
df_final = df.drop(*drop_columns)

# Write the transformed data back to S3
df_final.write.mode("overwrite").parquet("s3://amazonsales-capstone-sk/transformed/")

job.commit()


---------------------------------------------------------------------------------
{
  "product_id": "B08L12N5H1",
  "discounted_price": 2099,
  "actual_price": 2499,
  "discount_percentage": "16%",
  "rating": "|",
  "rating_count": "992",
  "brand_name": "Eureka",
  "main_category": "Home&Kitchen",
  "sub_category_1": "Kitchen&HomeAppliances",
  "sub_category_2": "Vacuum,Cleaning&Ironing",
  "sub_category_3": "Vacuums&FloorCare",
  "sub_category_4": "Vacuums",
  "above_4_rating": 0,
  "3to4_rating": 0,
  "bad_review": 0,
  "bad_review_percentage": 0.4101161995898838,
  "top_performer": 1
}

----------------------------------------
{
  "product_id": "B00ZRBWPA0",
  "discounted_price": 159,
  "actual_price": 180,
  "discount_percentage": "12%",
  "rating": "4.3",
  "rating_count": "989",
  "brand_name": "Eveready",
  "main_category": "Electronics",
  "sub_category_1": "GeneralPurposeBatteries&BatteryChargers",
  "sub_category_2": "DisposableBatteries",
  "above_4_rating": 0,
  "3to4_rating": 1,
  "bad_review": 0,
  "bad_review_percentage": 0.4101161995898838,
  "top_performer": 1
}


-------------------------------------------------------------------------------
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import split, col, when, udf
from pyspark.sql.types import FloatType, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from awsglue.job import Job

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Load the data into a DataFrame
df = spark.read.format("parquet").load("s3://amazonsales-capstone-sk/cleanedfiles/")

# Handle null values by replacing them with zeros
df = df.na.fill(0)

# Extract brand name from the first word of the product_name
df = df.withColumn("brand_name", split(col("product_name"), " ")[0])

# Extract product hierarchy from the category column
df = df.withColumn("main_category", split(col("category"), "\\|")[0]) \
    .withColumn("sub_category_1", split(col("category"), "\\|")[1]) \
    .withColumn("sub_category_2", split(col("category"), "\\|")[2]) \
    .withColumn("sub_category_3", split(col("category"), "\\|")[3]) \
    .withColumn("sub_category_4", split(col("category"), "\\|")[4])

# Calculate above_4_rating, 3to4_rating, and bad_review_percentage
df = df.withColumn("above_4_rating", when(col("rating") > 4, 1).otherwise(0)) \
    .withColumn("3to4_rating", when((col("rating") >= 3) & (col("rating") <= 4), 1).otherwise(0)) \
    .withColumn("bad_review", when(col("rating") < 3, 1).otherwise(0)) \
    .withColumn("bad_review_percentage", (F.sum("bad_review").over(Window.partitionBy())) / F.count("rating_count").over(Window.partitionBy()) * 100)

# Identify top performers
windowSpec = Window.orderBy(col("rating_count").desc())
df = df.withColumn("top_performer", F.when(F.rank().over(windowSpec) <= 10, 1).otherwise(0))

# Drop unnecessary columns
drop_columns = ['user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'about_product']
df_final = df.drop(*drop_columns)

# Write the transformed data back to S3
df_final.write.mode("overwrite").parquet("s3://amazonsales-capstone-sk/transformed/")

job.commit()



--------------------------------------------------------------
{
  "product_id": "B07JW9H4J1",
  "discounted_price(₹)": 399,
  "actual_price(₹)": 1099,
  "rating": "4.2",
  "rating_count": "24,269",
  "brand_name": "Wayona",
  "main_category": "Computers&Accessories",
  "sub_category_1": "Accessories&Peripherals",
  "sub_category_2": "Cables&Accessories",
  "sub_category_3": "Cables",
  "sub_category_4": "USBCables",
  "above_4_rating": 0,
  "3to4_rating": 1,
  "bad_review_percentage": 0,
  "top_performer": 1
}



----------------------------------------------------------------------------
successs;;;;;;;;;;;;

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import split, col, when, desc
from pyspark.sql.types import FloatType, StringType, IntegerType
from awsglue.job import Job

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Load the data into a DataFrame
df = spark.read.format("parquet").load("s3://amazonsales-capstone-sk/cleanedfiles/")

# Extract brand name from the first word of the product_name
df = df.withColumn("brand_name", split(col("product_name"), " ")[0])

# Extract product hierarchy from the category column
df = df.withColumn("main_category", split(col("category"), "\\|")[0]) \
    .withColumn("sub_category_1", split(col("category"), "\\|")[1]) \
    .withColumn("sub_category_2", split(col("category"), "\\|")[2]) \
    .withColumn("sub_category_3", split(col("category"), "\\|")[3]) \
    .withColumn("sub_category_4", split(col("category"), "\\|")[4])

# Calculate above_4_rating, 3to4_rating, and bad_review_percentage
df = df.withColumn("above_4_rating", when(col("rating") > 4, 1).otherwise(0)) \
    .withColumn("3to4_rating", when((col("rating") >= 3) & (col("rating") <= 4), 1).otherwise(0)) \
    .withColumn("bad_review_percentage", (1 - (col("above_4_rating") + col("3to4_rating"))) * 100)

# Convert discount_percentage to integer by removing the '%'
df = df.withColumn("discount_percentage", df["discount_percentage"].cast(IntegerType()))

# Calculate top performers
top_performers_list = df.groupBy("product_id").count().orderBy(desc("count")).limit(10).select("product_id").collect()
top_performers_list = [row.product_id for row in top_performers_list]
df = df.withColumn("top_performer", col("product_id").isin(top_performers_list).cast(IntegerType()))

# Rename discounted_price and actual_price columns
df = df.withColumnRenamed("discounted_price", "discounted_price(₹)") \
    .withColumnRenamed("actual_price", "actual_price(₹)")

# Drop unnecessary columns
drop_columns = ['user_id','category','product_name','user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'about_product']
df_final = df.drop(*drop_columns)

# Write the transformed data back to S3
df_final.write.mode("overwrite").parquet("s3://amazonsales-capstone-sk/transformed/")

job.commit()




----------------------------------------------------
///////import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import split, col, when, regexp_replace
from pyspark.sql.types import FloatType, StringType
from awsglue.job import Job

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Load the data into a DataFrame
df = spark.read.format("parquet").load("s3://amazonsales-capstone-sk/cleanedfiles/")

# Extract brand name from the first word of the product_name
df = df.withColumn("brand_name", split(col("product_name"), " ")[0])

# Extract product hierarchy from the category column
df = df.withColumn("main_category", split(col("category"), "\\|")[0]) \
    .withColumn("sub_category_1", split(col("category"), "\\|")[1]) \
    .withColumn("sub_category_2", split(col("category"), "\\|")[2]) \
    .withColumn("sub_category_3", split(col("category"), "\\|")[3]) \
    .withColumn("sub_category_4", split(col("category"), "\\|")[4])

# Convert discount_percentage to integer
df = df.withColumn("discount_percentage", regexp_replace(col("discount_percentage"), "%", "").cast(FloatType()))

# Convert rating to float and calculate above_4_rating, 3to4_rating, and bad_review_percentage
df = df.withColumn("rating", col("rating").cast(FloatType())) \
    .withColumn("above_4_rating", when(col("rating") > 4, 1).otherwise(0)) \
    .withColumn("3to4_rating", when((col("rating") >= 3) & (col("rating") <= 4), 1).otherwise(0)) \
    .withColumn("bad_review_percentage", (1 - (col("above_4_rating") + col("3to4_rating"))))

# Identify top performers
df = df.withColumn("top_performer", when(col("rating_count") > 100, 1).otherwise(0))

# Rename columns
df = df.withColumnRenamed("discounted_price", "discounted_price(₹)") \
    .withColumnRenamed("actual_price", "actual_price(₹)")

# Drop unnecessary columns
drop_columns = ['user_id','category','product_name','user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'about_product']
df_final = df.drop(*drop_columns)

# Write the transformed data back to S3
df_final.write.mode("overwrite").parquet("s3://amazonsales-capstone-sk/transformed/")

job.commit()


--------------------------------------------------------------------------------------
{
  "product_id": "B07JW9H4J1",
  "product_name": "Wayona Nylon Braided USB to Lightning Fast Charging and Data Sync Cable Compatible for iPhone 13, 12,11, X, 8, 7, 6, 5, iPad Air, Pro, Mini (3 FT Pack of 1, Grey)",
  "category": "Computers&Accessories|Accessories&Peripherals|Cables&Accessories|Cables|USBCables",
  "discounted_price": 399,
  "actual_price": 1099,
  "discount_percentage": "64%",
  "rating": "4.2",
  "rating_count": "24,269",
  "user_id": "AG3D6O4STAQKAY2UVGEUV46KN35Q,AHMY5CWJMMK5BJRBBSNLYT3ONILA,AHCTC6ULH4XB6YHDY6PCH2R772LQ,AGYHHIERNXKA6P5T7CZLXKVPT7IQ,AG4OGOFWXJZTQ2HKYIOCOY3KXF2Q,AENGU523SXMOS7JPDTW52PNNVWGQ,AEQJHCVTNINBS4FKTBGQRQTGTE5Q,AFC3FFC5PKFF5PMA52S3VCHOZ5FQ",
  "brand_name": "Wayona",
  "main_category": "Computers&Accessories",
  "sub_category_1": "Accessories&Peripherals",
  "sub_category_2": "Cables&Accessories",
  "sub_category_3": "Cables",
  "sub_category_4": "USBCables",
  "above_4_rating": 0,
  "3to4_rating": 1,
  "bad_review_percentage": 0
}





sucess.....>>>>>>
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import split, col, when
from pyspark.sql.types import FloatType, StringType
from awsglue.job import Job

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Load the data into a DataFrame
df = spark.read.format("parquet").load("s3://amazonsales-capstone-sk/cleanedfiles/")

# Extract brand name from the first word of the product_name
df = df.withColumn("brand_name", split(col("product_name"), " ")[0])

# Extract product hierarchy from the category column
df = df.withColumn("main_category", split(col("category"), "\\|")[0]) \
    .withColumn("sub_category_1", split(col("category"), "\\|")[1]) \
    .withColumn("sub_category_2", split(col("category"), "\\|")[2]) \
    .withColumn("sub_category_3", split(col("category"), "\\|")[3]) \
    .withColumn("sub_category_4", split(col("category"), "\\|")[4])

# Calculate above_4_rating, 3to4_rating, and bad_review_percentage
df = df.withColumn("above_4_rating", when(col("rating") > 4, 1).otherwise(0)) \
    .withColumn("3to4_rating", when((col("rating") >= 3) & (col("rating") <= 4), 1).otherwise(0)) \
    .withColumn("bad_review_percentage", (1 - (col("above_4_rating") + col("3to4_rating"))))

# Drop unnecessary columns
drop_columns = ['user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'about_product']
df_final = df.drop(*drop_columns)

# Write the transformed data back to S3
df_final.write.mode("overwrite").parquet("s3://amazonsales-capstone-sk/transformed/")

job.commit()



'''''''''''''''''''''''''
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import sys

# Initialize the Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Define the schema for the DataFrame
schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("discounted_price", FloatType(), True),
    StructField("actual_price", FloatType(), True),
    StructField("discount_percentage", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("rating_count", StringType(), True),
    StructField("about_product", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("review_id", StringType(), True),
    StructField("review_title", StringType(), True),
    StructField("review_content", StringType(), True),
    StructField("img_link", StringType(), True),
    StructField("product_link", StringType(), True)
])

# Load the data into a DataFrame
df = spark.read.format("parquet").schema(schema).load("s3://amazonsales-capstone-sk/cleanedfiles/")

# Extract brand name from the first word of the product_name
def extract_brand_name(product_name):
    return product_name.split(" ")[0]

# Create a UDF for the extract_brand_name function
extract_brand_name_udf = udf(extract_brand_name, StringType())

# Apply the UDF to create the brand_name column
df = df.withColumn("brand_name", extract_brand_name_udf(col("product_name")))

# Extract product hierarchy from the category column
def extract_product_hierarchy(category):
    categories = category.split('|')
    main_category = categories[0] if len(categories) > 0 else None
    sub_category_1 = categories[1] if len(categories) > 1 else None
    sub_category_2 = categories[2] if len(categories) > 2 else None
    sub_category_3 = categories[3] if len(categories) > 3 else None
    sub_category_4 = categories[4] if len(categories) > 4 else None
    return main_category, sub_category_1, sub_category_2, sub_category_3, sub_category_4

# Create a UDF for the extract_product_hierarchy function
product_hierarchy_udf = udf(extract_product_hierarchy, 
                            StructType([
                                StructField("main_category", StringType(), True),
                                StructField("sub_category_1", StringType(), True),
                                StructField("sub_category_2", StringType(), True),
                                StructField("sub_category_3", StringType(), True),
                                StructField("sub_category_4", StringType(), True)
                            ]))

# Apply the UDF to create the product hierarchy columns
df = df.withColumn("product_hierarchy", product_hierarchy_udf(col("category")))

# Expand the struct columns into separate columns
df = df.withColumn("main_category", col("product_hierarchy.main_category")) \
    .withColumn("sub_category_1", col("product_hierarchy.sub_category_1")) \
    .withColumn("sub_category_2", col("product_hierarchy.sub_category_2")) \
    .withColumn("sub_category_3", col("product_hierarchy.sub_category_3")) \
    .withColumn("sub_category_4", col("product_hierarchy.sub_category_4"))

# Calculate above_4_rating, 3to4_rating, and bad_review_percentage
df = df.withColumn("above_4_rating", when(col("rating") > 4, 1).otherwise(0)) \
    .withColumn("3to4_rating", when((col("rating") >= 3) & (col("rating") <= 4), 1).otherwise(0)) \
    .withColumn("bad_review_percentage", (1 - (col("above_4_rating") + col("3to4_rating"))))

# Drop unnecessary columns
drop_columns = ['user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'about_product']
df_final = df.drop(*drop_columns)

# Write the transformed data back to S3
df_final.write.mode("overwrite").parquet("s3://amazonsales-capstone-sk/transformed/")

job.commit()



==========================================================================
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import split, col, lit, udf, when
from pyspark.sql.types import FloatType, StringType, StructType, StructField
from awsglue.job import Job

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Define the schema for the DataFrame
schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("discounted_price", FloatType(), True),
    StructField("actual_price", FloatType(), True),
    StructField("discount_percentage", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("rating_count", StringType(), True),
    StructField("about_product", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("review_id", StringType(), True),
    StructField("review_title", StringType(), True),
    StructField("review_content", StringType(), True),
    StructField("img_link", StringType(), True),
    StructField("product_link", StringType(), True)
])

# Load the data into a DataFrame
df = spark.read.format("parquet").schema(schema).load("s3://amazonsales-capstone-sk/cleanedfiles/")

# Extract brand name from the first word of the product_name
def extract_brand_name(product_name):
    return product_name.split(" ")[0]

# Create a UDF for the extract_brand_name function
extract_brand_name_udf = udf(extract_brand_name, StringType())

# Apply the UDF to create the brand_name column
df = df.withColumn("brand_name", extract_brand_name_udf(col("product_name")))

# Extract product hierarchy from the category column
def extract_product_hierarchy(category):
    categories = category.split('|')
    main_category = categories[0] if len(categories) > 0 else None
    sub_category_1 = categories[1] if len(categories) > 1 else None
    sub_category_2 = categories[2] if len(categories) > 2 else None
    sub_category_3 = categories[3] if len(categories) > 3 else None
    sub_category_4 = categories[4] if len(categories) > 4 else None
    return main_category, sub_category_1, sub_category_2, sub_category_3, sub_category_4

# Create a UDF for the extract_product_hierarchy function
product_hierarchy_udf = udf(extract_product_hierarchy, 
                            StructType([
                                StructField("main_category": StringType(), True),
                                StructField("sub_category_1": StringType(),True),
                                StructField("sub_category_2": StringType(),True),
                                StructField("sub_category_3": StringType(),True),
                                StructField("sub_category_4": StringType(),True)
                             ]))

# Apply the UDF to create the product hierarchy columns
df = df.withColumn("product_hierarchy", product_hierarchy_udf(col("category")))

# Expand the struct columns into separate columns
df = df.withColumn("main_category", col("product_hierarchy.main_category")) \
    .withColumn("sub_category_1", col("product_hierarchy.sub_category_1")) \
    .withColumn("sub_category_2", col("product_hierarchy.sub_category_2")) \
    .withColumn("sub_category_3", col("product_hierarchy.sub_category_3")) \
    .withColumn("sub_category_4", col("product_hierarchy.sub_category_4"))

# Calculate above_4_rating, 3to4_rating, and bad_review_percentage
df = df.withColumn("above_4_rating", when(col("rating") > 4, 1).otherwise(0)) \
    .withColumn("3to4_rating", when((col("rating") >= 3) & (col("rating") <= 4), 1).otherwise(0)) \
    .withColumn("bad_review_percentage", (1 - (col("above_4_rating") + col("3to4_rating"))))

# Drop unnecessary columns
drop_columns = ['user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'about_product']
df_final = df.drop(*drop_columns)

# Write the transformed data back to S3
df_final.write.mode("overwrite").parquet("s3://amazonsales-capstone-sk/transformed/")

job.commit(),,, error:Error Category: SYNTAX_ERROR; SyntaxError: invalid syntax (amazonsales-gluejob-rsk.py, line 63)

------------------------------------------------------------
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import split, col, lit, udf, when
from pyspark.sql.types import FloatType, StringType, StructType, StructField
from awsglue.job import Job

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Define the schema for the DataFrame
schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("discounted_price", FloatType(), True),
    StructField("actual_price", FloatType(), True),
    StructField("discount_percentage", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("rating_count", StringType(), True),
    StructField("about_product", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("review_id", StringType(), True),
    StructField("review_title", StringType(), True),
    StructField("review_content", StringType(), True),
    StructField("img_link", StringType(), True),
    StructField("product_link", StringType(), True)
])

# Load the data into a DataFrame
df = spark.read.format("parquet").schema(schema).load("s3://amazonsales-capstone-sk/cleanedfiles/")

# Extract brand name from the first word of the product_name
def extract_brand_name(product_name):
    return product_name.split(" ")[0]

# Create a UDF for the extract_brand_name function
extract_brand_name_udf = udf(extract_brand_name, StringType())

# Apply the UDF to create the brand_name column
df = df.withColumn("brand_name", extract_brand_name_udf(col("product_name")))

# Extract product hierarchy from the category column
def extract_product_hierarchy(category):
    categories = category.split('|')
    main_category = categories[0] if len(categories) > 0 else None
    sub_category_1 = categories[1] if len(categories) > 1 else None
    sub_category_2 = categories[2] if len(categories) > 2 else None
    sub_category_3 = categories[3] if len(categories) > 3 else None
    sub_category_4 = categories[4] if len(categories) > 4 else None
    return main_category, sub_category_1, sub_category_2, sub_category_3, sub_category_4

# Create a UDF for the extract_product_hierarchy function
product_hierarchy_udf = udf(extract_product_hierarchy, 
                            ("main_category": StringType(),
                             "sub_category_1": StringType(),
                             "sub_category_2": StringType(),
                             "sub_category_3": StringType(),
                             "sub_category_4": StringType()))

# Apply the UDF to create the product hierarchy columns
df = df.withColumn("product_hierarchy", product_hierarchy_udf(col("category")))

# Expand the struct columns into separate columns
df = df.withColumn("main_category", col("product_hierarchy.main_category")) \
    .withColumn("sub_category_1", col("product_hierarchy.sub_category_1")) \
    .withColumn("sub_category_2", col("product_hierarchy.sub_category_2")) \
    .withColumn("sub_category_3", col("product_hierarchy.sub_category_3")) \
    .withColumn("sub_category_4", col("product_hierarchy.sub_category_4"))

# Calculate above_4_rating, 3to4_rating, and bad_review_percentage
df = df.withColumn("above_4_rating", when(col("rating") > 4, 1).otherwise(0)) \
    .withColumn("3to4_rating", when((col("rating") >= 3) & (col("rating") <= 4), 1).otherwise(0)) \
    .withColumn("bad_review_percentage", (1 - (col("above_4_rating") + col("3to4_rating"))))

# Drop unnecessary columns
drop_columns = ['user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'about_product']
df_final = df.drop(*drop_columns)

# Write the transformed data back to S3
df_final.write.mode("overwrite").parquet("s3://amazonsales-capstone-sk/transformed/")

job.commit()



------------------------------------------------------------------------------------
import sys
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

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_file_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles/"
s3_output_path = "s3://amazonsales-capstone-sk/transformed/"

input_file_path = args['input_file_path']
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

print(f"Writing transformed data to: {s3_output_path}")
data_transformed.write.mode("overwrite").parquet(s3_output_path)
print("Data saved successfully")

job.commit()





;;;;;;;;;;;prat[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[
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
s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles/"
s3_output_path = "s3://amazonsales-capstone-sk/transformed/"


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
  
    # Delete the input file
    file_system = file_status.getPath().getFileSystem(glueContext._jsc.hadoopConfiguration())
    file_system.delete(file_status.getPath(), True)
job.commit()


----------------------
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
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
s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles"
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



----------------------

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
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
s3_input_path = "s3://amazonsales-capstone-sk/cleanedfiles"
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
    if "discounted_price" in df.columns:
        df = df.withColumnRenamed("discounted_price", "discounted_price(₹)") \
               .withColumn("discounted_price(₹)", translate(col("discounted_price(₹)"), "₹,%", "").cast(FloatType()))
    if "actual_price" in df.columns:
        df = df.withColumnRenamed("actual_price", "actual_price(₹)") \
               .withColumn("actual_price(₹)", translate(col("actual_price(₹)"), "₹,%", "").cast(FloatType()))
    return df

# Function to clean discount_percentage column
def clean_discount_percentage(df):
    if "discount_percentage" in df.columns:
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
           .withColumn("rating_count", remove_symbols(col("rating_count")) \
           .cast(IntegerType()))
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

# Write results to S3 as a single Parquet file
df_cleaned.write.parquet(s3_output_path, mode="overwrite")
