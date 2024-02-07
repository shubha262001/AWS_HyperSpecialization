import boto3
import pandas as pd
import os
from io import StringIO

s3_client = boto3.client('s3')

def is_csv(file_path):
    # Check if the file is in CSV format
    return file_path.lower().endswith('.csv')

def has_required_columns(file_path):
    # Check if the CSV file has all required columns
    required_columns = ['product_id', 'product_name', 'category', 'discounted_price', 'actual_price', 'discount_percentage', 'rating', 'rating_count', 'about_product', 'user_id', 'user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link']
    df = pd.read_csv(file_path)
    return all(col in df.columns for col in required_columns)

def standardize_data(file_path):
    # Standardize the data by removing junk characters, etc.
    df = pd.read_csv(file_path)
    # Remove special characters from 'discounted_price' and 'actual_price' columns
    df['discounted_price'] = df['discounted_price'].str.replace('₹', '').astype(float)
    df['actual_price'] = df['actual_price'].str.replace('₹', '').astype(float)
    # Remove '%' sign from 'discount_percentage' column and convert to float
    df['discount_percentage'] = df['discount_percentage'].str.rstrip('%').astype(float)
    # Convert 'rating' column to float
    df['rating'] = df['rating'].astype(float)
    return df

def lambda_handler(event, context):
    # Get uploaded file details
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    download_path = '/tmp/{}'.format(key)
    
    # Download CSV file from S3
    s3_client.download_file(bucket, key, download_path)
    
    # Perform data quality checks
    if is_csv(download_path) and has_required_columns(download_path):
        # If checks pass, standardize data
        standardized_data = standardize_data(download_path)
        
        # Upload the cleaned data as a Parquet file to a different location
        parquet_key = 'processed/{}.parquet'.format(os.path.splitext(key)[0])
        upload_path = '/tmp/{}.parquet'.format(os.path.splitext(key)[0])
        standardized_data.to_parquet(upload_path, index=False)
        s3_client.upload_file(upload_path, bucket, parquet_key)
        
        # Delete the original CSV file from S3
        s3_client.delete_object(Bucket=bucket, Key=key)
    else:
        # If checks fail, move the file to the error folder
        error_folder = 'error'
        error_key = '{}/{}'.format(error_folder, key)
        s3_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=error_key)
        s3_client.delete_object(Bucket=bucket, Key=key)
    
    return {
        'statusCode': 200,
        'body': 'Data processed successfully!'
    }
