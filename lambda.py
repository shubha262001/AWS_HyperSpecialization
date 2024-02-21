import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
from datetime import datetime, timedelta

s3 = boto3.client('s3')
logs = boto3.client('logs')

def lambda_handler(event, context):
    # Get the bucket name and the object key from the event
    bucket_name = "sk-amazonsales-capstone"
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
            
            # Trigger the Glue job
            trigger_glue_job(bucket_name, cleaned_object_key)
            print("Glue job triggered successfully.")

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

    # Get logs from CloudWatch Logs
    try:
        log_group = '/aws/lambda/your-lambda-function-name'  # Replace with your Lambda function name
        start_query_response = logs.start_query(
            logGroupName=log_group,
            startTime=int((datetime.today() - timedelta(hours=1)).timestamp()) * 1000,
            endTime=int(datetime.now().timestamp()) * 1000,
            queryString='fields @timestamp, @message | sort @timestamp desc | limit 20',
        )
        query_id = start_query_response['queryId']

        while True:
            query_status = logs.get_query_results(queryId=query_id)
            if query_status['status'] == 'Complete':
                break
            time.sleep(1)

        query_results = logs.get_query_results(queryId=query_id)
        log_messages = [r['value'] for r in query_results['results']]
        
        # Save log messages to S3
        log_key = f"cloudwatchlogs/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.txt"
        log_data = '\n'.join(log_messages).encode('utf-8')
        s3.put_object(Bucket=bucket_name, Key=log_key, Body=log_data)
        print("CloudWatch Logs saved to S3 successfully.")

    except Exception as e:
        print(f"An error occurred while fetching or saving CloudWatch Logs: {str(e)}")

def trigger_glue_job(bucket_name, object_key):
    # Check if the file is a parquet and in the cleanedfiles folder
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start






--------------------------------------------------------------------------------------------------------------------------------------
No older events at this moment. 
Retry

2024-02-21T15:24:28.168+05:30	INIT_START Runtime Version: python:3.12.v18 Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:776a3759221679a634181f858871d5514dc74a176f78bc535f822a932845ae5a

2024-02-21T15:24:28.250+05:30	/var/task/lambda_function.py:37: SyntaxWarning: invalid escape sequence '\d'

2024-02-21T15:24:28.250+05:30	df['discounted_price'] = df['discounted_price'].str.replace('[^\d.]', '', regex=True).astype(float)

2024-02-21T15:24:28.250+05:30	/var/task/lambda_function.py:38: SyntaxWarning: invalid escape sequence '\d'

2024-02-21T15:24:28.250+05:30	df['actual_price'] = df['actual_price'].str.replace('[^\d.]', '', regex=True).astype(float)

2024-02-21T15:24:30.874+05:30	START RequestId: be06c57a-1350-4649-8159-36efa3994176 Version: $LATEST

2024-02-21T15:24:38.464+05:30	Glue job started: {'JobRunId': 'jr_98b5a86f9154d849925668e8368734e38c6a320899b35a0457b9e9c61aade12b', 'ResponseMetadata': {'RequestId': 'a5d771d2-147d-4ff0-8063-7ddf80fb2750', 'HTTPStatusCode': 200, 'HTTPHeaders': {'date': 'Wed, 21 Feb 2024 09:54:38 GMT', 'content-type': 'application/x-amz-json-1.1', 'content-length': '82', 'connection': 'keep-alive', 'x-amzn-requestid': 'a5d771d2-147d-4ff0-8063-7ddf80fb2750'}, 'RetryAttempts': 0}}

2024-02-21T15:24:38.466+05:30	Glue job triggered successfully.

2024-02-21T15:24:38.474+05:30	An error occurred: name 'datetime' is not defined

2024-02-21T15:24:39.137+05:30	END RequestId: be06c57a-1350-4649-8159-36efa3994176

2024-02-21T15:24:39.137+05:30	REPORT RequestId: be06c57a-1350-4649-8159-36efa3994176 Duration: 8263.16 ms Billed Duration: 8264 ms Memory Size: 128 MB Max Memory Used: 128 MB Init Duration: 2703.97 ms
------------------------------------------------------------------
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time

s3 = boto3.client('s3')
logs = boto3.client('logs')

def lambda_handler(event, context):
    # Get the bucket name and the object key from the event
    bucket_name = "sk-amazonsales-capstone"
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
            
            # Trigger the Glue job
            trigger_glue_job(bucket_name, cleaned_object_key)
            print("Glue job triggered successfully.")

            # Move CloudWatch logs to S3
            move_cloudwatch_logs(bucket_name, object_key)
            print("CloudWatch logs moved successfully.")

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

def trigger_glue_job(bucket_name, object_key):
    # Check if the file is a parquet and in the cleanedfiles folder
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started:", response)
        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")
    else:
        print("File is not in the cleanedfiles folder or not a parquet")

def move_cloudwatch_logs(bucket_name, log_group_name):
    # Get logs from CloudWatch
    start_query_response = logs.start_query(
        logGroupName=log_group_name,
        startTime=int((datetime.today() - timedelta(hours=24)).timestamp()),
        endTime=int(datetime.now().timestamp()),
        queryString='fields @timestamp, @message',
    )

    query_id = start_query_response['queryId']

    # Wait for query to complete
    while True:
        query_status = logs.get_query_results(queryId=query_id)
        if query_status['status'] == 'Complete':
            break
        time.sleep(1)

    # Write logs to S3
    response = logs.get_query_results(queryId=query_id)
    log_data = '\n'.join([event['@message'] for event in response['results']])
    s3.put_object(Bucket=bucket_name, Key='cloudwatchlogs/' + log_group_name + '.log', Body=log_data.encode())






-------------------------------------------------------------------------------

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the bucket name and the object key from the event
    bucket_name = "sk-amazonsales-capstone"
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
            
            # Trigger the Glue job
            trigger_glue_job(bucket_name, cleaned_object_key)
            
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

def trigger_glue_job(bucket_name, object_key):
    # Check if the file is a parquet and in the cleanedfiles folder
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started:", response)
        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")
    else:
        print("File is not in the cleanedfiles folder or not a parquet")



------------------------------------------------------------------------------
An error occurred: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the bucket name and the object key from the event
    bucket_name = "sk-amazonsales-capstone"
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

def trigger_glue_job(bucket_name, object_key):
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started:", response)
        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")
    else:
        print("File is not in the cleanedfiles folder or not a parquet")

------------------------------------

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the bucket name and the object key from the event
    bucket_name = "sk-amazonsales-capstone"
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
            
            # Trigger the Glue job
            trigger_glue_job(bucket_name, cleaned_object_key)
            
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

def trigger_glue_job(bucket_name, object_key):
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started:", response)
        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")
    else:
        print("File is not in the cleanedfiles folder or not a parquet")




---------------------------------------------------------------------------------------------------------------------------------
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the bucket name and the object key from the event
    bucket_name = "sk-amazonsales-capstone"
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
        
        #glue job trigger
         # Check if the file is a parquet and in the cleanedfiles folder
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started:", response)
        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")
    else:
        print("File is not in the cleanedfiles folder or not a parquet")
-----------------------------------------------------------------------------------
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

def lambda_handler(event, context):
    # Get the bucket name and the object key from the event
    bucket_name = "amazonsales-capstone-sk"
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Create an S3 client
    s3 = boto3.client('s3')
    
    # Check if the file is a CSV
    if object_key.endswith('.csv'):
        try:
            # Get the object from S3
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            csv_data = response['Body'].read().decode('utf-8')
            
            # Read CSV data into a DataFrame
            df = pd.read_csv(io.StringIO(csv_data))
            
            # Verify the presence of all required data columns
            required_columns = ['product_id', 'product_name', 'category', 'discounted_price','actual_price', 'discount_percentage', 'rating', 'rating_count','about_product', 'user_id', 'user_name', 'review_id', 'review_title','review_content', 'img_link', 'product_link']
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
            
            # Trigger the Glue job
            job_name = "amazonsales-sk-job"
            glue = boto3.client('glue')
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started:", response)
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





==================================================================
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

def lambda_handler(event, context):
    # Get the bucket name and the object key from the event
    bucket_name = "amazonsales-capstone-sk"
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Create an S3 client
    s3 = boto3.client('s3')
    
    # Check if the file is a CSV
    if object_key.endswith('.csv'):
        try:
            # Get the object from S3
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            csv_data = response['Body'].read().decode('utf-8')
            
            # Read CSV data into a DataFrame
            df = pd.read_csv(io.StringIO(csv_data))
            
            # Verify the presence of all required data columns
            required_columns = ['product_id', 'product_name', 'category', 'discounted_price','actual_price', 'discount_percentage', 'rating', 'rating_count','about_product', 'user_id', 'user_name', 'review_id', 'review_title','review_content', 'img_link', 'product_link']
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
            
            # Trigger the Glue job
            job_name = "amazonsales-sk-job"
            glue = boto3.client('glue')
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started:", response)
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








--------------------------------------------------------------------------------------------------




import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the bucket name and the object ke1y from the event
    bucket_name = "aamazonsales-capstone-sk"
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Check if the file is a CSV
    if object_key.startswith('allfiles/') and object_key.endswith('.csv'):
        try:
            # Get the object from S3
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            csv_data = response['Body'].read().decode('utf-8')
            
            # Read CSV data into a DataFrame
            df = pd.read_csv(io.StringIO(csv_data))
            
            # Clean the price column
            df['discounted_price'] = df['discounted_price'].str.replace('₹', '').str.replace(',', '').astype(float)
            df['actual_price'] = df['actual_price'].str.replace('₹', '').str.replace(',', '').astype(float)
            
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
        # Move the file to the error folder if it's not in the correct folder or not a CSV
        error_object_key = 'errorfiles/' + object_key.split('/')[-1]
        s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})
        s3.delete_object(Bucket=bucket_name, Key=object_key)


























