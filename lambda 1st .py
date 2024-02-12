import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the bucket name and the object key from the event
    bucket_name = "amazonsales-capstone-sk"
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Check if the file is a CSV
    if object_key.endswith('.csv'):
        try:
            # Get the object from S3
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            csv_data = response['Body'].read().decode('utf-8')
            
            # Read CSV data into a DataFrame
            df = pd.read_csv(io.StringIO(csv_data))
            
            # Verify the presence of all required data columns
            required_columns = ['product_id', 'discounted_price', 'actual_price', 'rating', 'rating_count', 'category']
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                # Move the file to the error folder if any required column is missing
                error_object_key = 'errorfiles/' + object_key
                s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})
                s3.delete_object(Bucket=bucket_name, Key=object_key)
                return
            
            # Clean the price columns and remove junk and special characters
            df['discounted_price'] = df['discounted_price'].str.replace('[^\d.]', '', regex=True).astype(float)
            df['actual_price'] = df['actual_price'].str.replace('[^\d.]', '', regex=True).astype(float)
            
            # Convert DataFrame to Parquet format
            table = pa.Table.from_pandas(df)
            parquet_file = io.BytesIO()
            pq.write_table(table, parquet_file)
            parquet_file.seek(0)
            
            # Upload the Parquet file to the cleaned files folder
            cleaned_object_key = 'cleanedfiles/' + object_key.split('/')[-1].replace('.csv', '.parquet')
            s3.put_object(Bucket=bucket_name, Key=cleaned_object_key, Body=parquet_file)
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            # Move the file to the error folder if an error occurs
            error_object_key = 'errorfiles/' + object_key
            s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})
            s3.delete_object(Bucket=bucket_name, Key=object_key)
    else:
        # Move the file to the error folder if it's not a CSV
        error_object_key = 'errorfiles/' + object_key
        s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})
        s3.delete_object(Bucket=bucket_name, Key=object_key)
