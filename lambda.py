import boto3
from datetime import datetime, timedelta
import json
import time

def lambda_handler(event, context):
    client = boto3.client('logs')
    s3 = boto3.client('s3')
    
    query = """fields @timestamp, @message | sort @timestamp desc | LIMIT 5"""

    log_group = '/aws/lambda/sk-func-amazonsales-capstone'
    bucket_name = "amazonsales-capstone-sk"
    s3_key = "cloudwatchlogs/{}.json".format(datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))

    start_query_response = client.start_query(
        logGroupName=log_group,
        startTime=int((datetime.today() - timedelta(hours=24)).timestamp()),
        endTime=int(datetime.now().timestamp()),
        queryString=query,
    )

    query_id = start_query_response['queryId']

    response = None

    while response is None or response['status'] == 'Running':
        print('Waiting for query to complete ...')
        time.sleep(1)
        response = client.get_query_results(queryId=query_id)

    response_string = str(response).replace("'", '"')
    json_object = json.loads(response_string)

    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=json.dumps(json_object, indent=4))

    return {
        'statusCode': 200,
        'body': "Success"
    }



------------------------------------------------
import boto3
import os
import time

s3 = boto3.client('s3')
logs = boto3.client('logs')

def lambda_handler(event, context):
    log_group = '/aws/lambda/sk-func-amazonsales-capstone'
    bucket_name = "amazonsales-capstone-sk"
    
    try:
        start_query_response = logs.start_query(
            logGroupName=log_group,
            startTime=int((time.time() - 3600) * 1000),  # Start time 1 hour ago
            endTime=int(time.time() * 1000),  # End time now
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
        log_key = f"cloudwatchlogs/{time.strftime('%Y-%m-%d-%H-%M-%S')}.txt"
        log_data = '\n'.join(log_messages).encode('utf-8')
        s3.put_object(Bucket=bucket_name, Key=log_key, Body=log_data)
        print("CloudWatch Logs saved to S3 successfully.")

    except Exception as e:
        print(f"An error occurred while fetching or saving CloudWatch Logs: {str(e)}")




--------------------------------------------------------------------------------------------------------------

import boto3
import os
from pprint import pprint
import time

s3 = boto3.client('s3')
logs = boto3.client('logs')
ssm = boto3.client('ssm')

def lambda_handler(event, context):
    extra_args = {}
    log_groups = []
    log_groups_to_export = []
    bucket_name = "amazonsales-capstone-sk"  # Your S3 bucket name

    print("--> S3_BUCKET=%s" % bucket_name)

    while True:
        response = logs.describe_log_groups(**extra_args)
        log_groups = log_groups + response['logGroups']
        
        if not 'nextToken' in response:
            break
        extra_args['nextToken'] = response['nextToken']
    
    for log_group in log_groups:
        response = logs.list_tags_log_group(logGroupName=log_group['logGroupName'])
        log_group_tags = response['tags']
        if 'ExportToS3' in log_group_tags and log_group_tags['ExportToS3'] == 'true':
            log_groups_to_export.append(log_group['logGroupName'])
    
    for log_group_name in log_groups_to_export:
        ssm_parameter_name = ("/log-exporter-last-export/%s" % log_group_name).replace("//", "/")
        try:
            ssm_response = ssm.get_parameter(Name=ssm_parameter_name)
            ssm_value = ssm_response['Parameter']['Value']
        except ssm.exceptions.ParameterNotFound:
            ssm_value = "0"
        
        export_to_time = int(round(time.time() * 1000))
        
        print("--> Exporting %s to %s" % (log_group_name, bucket_name))
        
        if export_to_time - int(ssm_value) < (24 * 60 * 60 * 1000):
            # Haven't been 24hrs from the last export of this log group
            print("    Skipped until 24hrs from last export is completed")
            continue
        
        try:
            response = logs.create_export_task(
                logGroupName=log_group_name,
                fromTime=int(ssm_value),
                to=export_to_time,
                destination=bucket_name,
                destinationPrefix=log_group_name.strip("/")
            )
            print("    Task created: %s" % response['taskId'])
            time.sleep(5)
            
        except logs.exceptions.LimitExceededException:
            print("    Need to wait until all tasks are finished (LimitExceededException). Continuing later...")
            return
        
        except Exception as e:
            print("    Error exporting %s: %s" % (log_group_name, getattr(e, 'message', repr(e))))
            continue
        
        ssm_response = ssm.put_parameter(
            Name=ssm_parameter_name,
            Type="String",
            Value=str(export_to_time),
            Overwrite=True)

-------------------------------------------------
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
from datetime import datetime, timedelta

s3 = boto3.client('s3')
logs = boto3.client('logs')
ssm = boto3.client('ssm')

def lambda_handler(event, context):
    bucket_name = "amazonsales-capstone-sk"
    log_group = '/aws/lambda/sk-func-amazonsales-capstone'
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
            required_columns = ['product_id', 'product_name', 'category', 'discounted_price','actual_price', 'discount_percentage', 'rating', 'rating_count','about_product', 'user_id', 'user_name', 'review_id', 'review_title','review_content', 'img_link', 'product_link']
            
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                # Move the file to the error folder if any required column is missing
                error_object_key = 'errorfiles/' + object_key
                s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})
                s3.delete_object(Bucket=bucket_name, Key=object_key)
                return
            
            # Clean the price columns and remove junk and special characters
            df['discounted_price'] = df['discounted_price'].str.replace(r'[^\d.]', '', regex=True).astype(float)
            df['actual_price'] = df['actual_price'].str.replace(r'[^\d.]', '', regex=True).astype(float)
            
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

    # Fetch and save logs
    fetch_and_save_logs()

def trigger_glue_job(bucket_name, object_key):
# Check if the file is a parquet and in the cleanedfiles folder
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started successfully.")

        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")

    else:
        print("File is not in the cleanedfiles folder or not a parquet")


def fetch_and_save_logs():
    try:
        extra_args = {}
        log_groups = []
        log_groups_to_export = []

        if 'S3_BUCKET' not in os.environ:
            print("Error: S3_BUCKET not defined")
            return

        print("--> S3_BUCKET=%s" % os.environ["S3_BUCKET"])

        while True:
            response = logs.describe_log_groups(**extra_args)
            log_groups = log_groups + response['logGroups']

            if not 'nextToken' in response:
                break
            extra_args['nextToken'] = response['nextToken']

        for log_group in log_groups:
            response = logs.list_tags_log_group(logGroupName=log_group['logGroupName'])
            log_group_tags = response['tags']
            if 'ExportToS3' in log_group_tags and log_group_tags['ExportToS3'] == 'true':
                log_groups_to_export.append(log_group['logGroupName'])

        for log_group_name in log_groups_to_export:
            ssm_parameter_name = ("/log-exporter-last-export/%s" % log_group_name).replace("//", "/")
            try:
                ssm_response = ssm.get_parameter(Name=ssm_parameter_name)
                ssm_value = ssm_response['Parameter']['Value']
            except ssm.exceptions.ParameterNotFound:
                ssm_value = "0"

            export_to_time = int(round(time.time() * 1000))

            print("--> Exporting %s to %s" % (log_group_name, os.environ['S3_BUCKET']))

            if export_to_time - int(ssm_value) < (24 * 60 * 60 * 1000):
                # Haven't been 24hrs from the last export of this log group
                print("    Skipped until 24hrs from last export is completed")
                continue

            try:
                response = logs.create_export_task(
                    logGroupName=log_group_name,
                    fromTime=int(ssm_value),
                    to=export_to_time,
                    destination=os.environ['S3_BUCKET'],
                    destinationPrefix=log_group_name.strip("/")
                )
                print("    Task created: %s" % response['taskId'])
                time.sleep(5)

            except logs.exceptions.LimitExceededException:
                print("    Need to wait until all tasks are finished (LimitExceededException). Continuing later...")
                return

            except Exception as e:
                print("    Error exporting %s: %s" % (log_group_name, getattr(e, 'message', repr(e))))
                continue

            ssm_response = ssm.put_parameter(
                Name=ssm_parameter_name,
                Type="String",
                Value=str(export_to_time),
                Overwrite=True)

    except Exception as e:
        print(f"An error occurred while fetching or saving CloudWatch Logs: {str(e)}")



--------------------------------------------------------------------------
def fetch_and_save_logs():
    try:
        start_query_response = logs.start_query(
            logGroupName=log_group,
            startTime=int((datetime.today() - timedelta(hours=1)).timestamp()) * 1000,
            endTime=int(datetime.now().timestamp()) * 1000,
            queryString='fields @timestamp, @message | sort @timestamp desc | limit 20',
        )
        query_id = start_query_response['queryId']

        print("Query ID:", query_id)

        while True:
            query_status = logs.get_query_results(queryId=query_id)
            if query_status['status'] == 'Complete':
                break
            print("Query status:", query_status['status'])
            time.sleep(1)

        query_results = logs.get_query_results(queryId=query_id)
        if 'results' in query_results and isinstance(query_results['results'], list):
            log_messages = [r['value'] for r in query_results['results']]
        else:
            log_messages = []
        
        # Save log messages to S3
        log_key = f"cloudwatchlogs/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.txt"
        log_data = '\n'.join(log_messages).encode('utf-8')
        s3.put_object(Bucket=bucket_name, Key=log_key, Body=log_data)
        print("CloudWatch Logs saved to S3 successfully.")

    except Exception as e:
        print(f"An error occurred while fetching or saving CloudWatch Logs: {str(e)}")

=======================================================================================================
=Timestamp
Message
No older events at this moment. 
Retry

2024-02-21T18:42:30.798+05:30	INIT_START Runtime Version: python:3.12.v18 Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:776a3759221679a634181f858871d5514dc74a176f78bc535f822a932845ae5a

2024-02-21T18:42:33.933+05:30	Query status: Scheduled

2024-02-21T18:42:35.036+05:30	Query status: Running

2024-02-21T18:42:36.241+05:30
An error occurred while fetching or saving CloudWatch Logs: list indices must be integers or slices, not str

Copy
An error occurred while fetching or saving CloudWatch Logs: list indices must be integers or slices, not str

2024-02-21T18:42:36.252+05:30	START RequestId: 78a915ea-29b5-453f-a456-63f63b453358 Version: $LATEST

2024-02-21T18:42:43.847+05:30	Glue job started successfully.

2024-02-21T18:42:43.849+05:30	Glue job triggered successfully.

2024-02-21T18:42:44.489+05:30	END RequestId: 78a915ea-29b5-453f-a456-63f63b453358

2024-02-21T18:42:44.489+05:30	REPORT RequestId: 78a915ea-29b5-453f-a456-63f63b453358 Duration: 8237.39 ms Billed Duration: 8238 ms

---------------------------------------------------------------------------
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
from datetime import datetime, timedelta

s3 = boto3.client('s3')
logs = boto3.client('logs')
bucket_name = "amazonsales-capstone-sk"
log_group = '/aws/lambda/sk-func-amazonsales-capstone'

def lambda_handler(event, context):
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
            required_columns = ['product_id', 'product_name', 'category', 'discounted_price', 'actual_price', 'discount_percentage', 'rating', 'rating_count', 'about_product', 'user_id', 'user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link']
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                # Move the file to the error folder if any required column is missing
                error_object_key = 'errorfiles/' + object_key
                s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})
                s3.delete_object(Bucket=bucket_name, Key=object_key)
                return
            
            # Clean the price columns and remove junk and special characters
            df['discounted_price'] = df['discounted_price'].str.replace(r'[^\d.]', '', regex=True).astype(float)
            df['actual_price'] = df['actual_price'].str.replace(r'[^\d.]', '', regex=True).astype(float)
            
            # Convert DataFrame to Parquet format
            table = pa.Table.from_pandas(df)
            parquet_file = io.BytesIO()
            pq.write_table(table, parquet_file)
            parquet_file.seek(0)
            
            # Upload the Parquet file to the cleaned files folder
            cleaned_object_key = 'cleanedfiles/' + object_key.split('/')[-1].replace('.csv', '.parquet')
            s3.put_object(Bucket=bucket_name, Key=cleaned_object_key, Body=parquet_file)
            
            # Trigger the Glue job
            trigger_glue_job(cleaned_object_key)
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

def trigger_glue_job(object_key):
    # Check if the file is a parquet and in the cleanedfiles folder
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started successfully.")

        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")

    else:
        print("File is not in the cleanedfiles folder or not a parquet")

# Separate function for fetching and saving CloudWatch Logs
def fetch_and_save_logs():
    try:
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

# Call the function to fetch and save logs
fetch_and_save_logs()

---------------------------------------------------------------------------------------------------------------------
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
from datetime import datetime, timedelta

s3 = boto3.client('s3')
logs = boto3.client('logs')

def lambda_handler(event, context):
    bucket_name = "amazonsales-capstone-sk"
    log_group = '/aws/lambda/sk-func-amazonsales-capstone'
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
            required_columns = ['product_id', 'product_name', 'category', 'discounted_price','actual_price', 'discount_percentage', 'rating', 'rating_count','about_product', 'user_id', 'user_name', 'review_id', 'review_title','review_content', 'img_link', 'product_link']
            
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                # Move the file to the error folder if any required column is missing
                error_object_key = 'errorfiles/' + object_key
                s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})
                s3.delete_object(Bucket=bucket_name, Key=object_key)
                return
            
            # Clean the price columns and remove junk and special characters
            df['discounted_price'] = df['discounted_price'].str.replace(r'[^\d.]', '', regex=True).astype(float)
            df['actual_price'] = df['actual_price'].str.replace(r'[^\d.]', '', regex=True).astype(float)
            
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

def trigger_glue_job(bucket_name, object_key):
    # Check if the file is a parquet and in the cleanedfiles folder
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started successfully.")

        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")

    else:
        print("File is not in the cleanedfiles folder or not a parquet")

    # Get logs from CloudWatch Logs
    try:
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


----------------------------------------------------------------------------------------------------------------------
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
from datetime import datetime, timedelta

s3 = boto3.client('s3')
logs = boto3.client('logs')

def lambda_handler(event, context):
    bucket_name = "amazonsales-capstone-sk"
    log_group = '/aws/lambda/sk-func-amazonsales-capstone'
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
            df['discounted_price'] = df['discounted_price'].str.replace(r'[^\d.]', '', regex=True).astype(float)
            df['actual_price'] = df['actual_price'].str.replace(r'[^\d.]', '', regex=True).astype(float)
            
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

def trigger_glue_job(bucket_name, object_key):
    # Check if the file is a parquet and in the cleanedfiles folder
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started successfully.")

        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")

    else:
        print("File is not in the cleanedfiles folder or not a parquet")

    # Get logs from CloudWatch Logs
    try:
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



-------------------------------------------------------------------------------------------------------------
An error occurred while fetching or saving CloudWatch Logs: list indices must be integers or slices, not str

--------------------------------------------------------------------------------------------------
No older events at this moment. 
Retry

2024-02-21T16:38:28.832+05:30	INIT_START Runtime Version: python:3.12.v18 Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:776a3759221679a634181f858871d5514dc74a176f78bc535f822a932845ae5a

2024-02-21T16:38:28.921+05:30	/var/task/lambda_function.py:37: SyntaxWarning: invalid escape sequence '\d'

2024-02-21T16:38:28.921+05:30	df['discounted_price'] = df['discounted_price'].str.replace('[^\d.]', '', regex=True).astype(float)

2024-02-21T16:38:28.921+05:30	/var/task/lambda_function.py:38: SyntaxWarning: invalid escape sequence '\d'

2024-02-21T16:38:28.921+05:30	df['actual_price'] = df['actual_price'].str.replace('[^\d.]', '', regex=True).astype(float)

2024-02-21T16:38:31.790+05:30	START RequestId: a2467a0c-e08c-4215-99c4-5db31eaef9ca Version: $LATEST

2024-02-21T16:38:39.644+05:30	Glue job started successfully.

2024-02-21T16:38:40.334+05:30	An error occurred while fetching or saving CloudWatch Logs: name 'time' is not defined

2024-02-21T16:38:40.335+05:30	Glue job triggered successfully.

2024-02-21T16:38:40.852+05:30	END RequestId: a2467a0c-e08c-4215-99c4-5db31eaef9ca

2024-02-21T16:38:40.852+05:30	REPORT RequestId: a2467a0c-e08c-4215-99c4-5db31eaef9ca Duration: 9062.78 ms Billed Duration: 9063 ms M

------------------------------------------------------------------
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime, timedelta

s3 = boto3.client('s3')
logs = boto3.client('logs')

def lambda_handler(event, context):
    bucket_name = "amazonsales-capstone-sk"
    log_group = '/aws/lambda/sk-func-amazonsales-capstone'
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

def trigger_glue_job(bucket_name, object_key):
    # Check if the file is a parquet and in the cleanedfiles folder
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started successfully.")

        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")

    else:
        print("File is not in the cleanedfiles folder or not a parquet")

    # Get logs from CloudWatch Logs
    try:
        start_query_response = logs.start_query(
            logGroupName='/aws/lambda/sk-func-amazonsales-capstone',
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


--------------------------------------------------------------------------------------------------------------------------------------
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime, timedelta

s3 = boto3.client('s3')
logs = boto3.client('logs')

def lambda_handler(event, context):
    bucket_name = "amazonsales-capstone-sk"
    log_group = '/aws/lambda/sk-func-amazonsales-capstone'
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

def trigger_glue_job(bucket_name, object_key):
    # Check if the file is a parquet and in the cleanedfiles folder
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started successfully.")

        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")

    else:
        print("File is not in the cleanedfiles folder or not a parquet")

    # Get logs from CloudWatch Logs
    try:
        start_query_response = logs.start_query(
            logGroupName=/aws/lambda/sk-func-amazonsales-capstone,
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



error: 	No older events at this moment. 
Retry

2024-02-21T16:30:43.109+05:30	INIT_START Runtime Version: python:3.12.v18 Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:776a3759221679a634181f858871d5514dc74a176f78bc535f822a932845ae5a

2024-02-21T16:30:43.194+05:30	/var/task/lambda_function.py:37: SyntaxWarning: invalid escape sequence '\d'

2024-02-21T16:30:43.194+05:30	df['discounted_price'] = df['discounted_price'].str.replace('[^\d.]', '', regex=True).astype(float)

2024-02-21T16:30:43.194+05:30	/var/task/lambda_function.py:38: SyntaxWarning: invalid escape sequence '\d'

2024-02-21T16:30:43.194+05:30	df['actual_price'] = df['actual_price'].str.replace('[^\d.]', '', regex=True).astype(float)

2024-02-21T16:30:43.195+05:30	/var/task/lambda_function.py:37: SyntaxWarning: invalid escape sequence '\d'

2024-02-21T16:30:43.195+05:30	df['discounted_price'] = df['discounted_price'].str.replace('[^\d.]', '', regex=True).astype(float)

2024-02-21T16:30:43.195+05:30	/var/task/lambda_function.py:38: SyntaxWarning: invalid escape sequence '\d'

2024-02-21T16:30:43.195+05:30	df['actual_price'] = df['actual_price'].str.replace('[^\d.]', '', regex=True).astype(float)

2024-02-21T16:30:43.198+05:30	START RequestId: 61b8efe2-766b-402c-acdc-31514834c3f5 Version: $LATEST

2024-02-21T16:30:43.199+05:30	[ERROR] Runtime.UserCodeSyntaxError: Syntax error in module 'lambda_function': invalid syntax (lambda_function.py, line 85) Traceback (most recent call last):   File "/var/task/lambda_function.py" Line 85                 logGroupName=/aws/lambda/sk-func-amazonsales-capstone,

2024-02-21T16:30:43.219+05:30	END RequestId: 61b8efe2-766b-402c-acdc-31514834c3f5

2024-02-21T16:30:43.219+05:30	REPORT RequestId: 61b8efe2-766b-402c-acdc-31514834c3f5 Duration: 20.64 ms Billed Duration: 21 ms Memory Size: 128 MB Max Memory Used: 34 MB Init Duration: 88.58 ms

2024-02-21T16:31:44.373+05:30	START RequestId: 61b8efe2-766b-402c-acdc-31514834c3f5 Version: $LATEST

2024-02-21T16:31:44.373+05:30	[ERROR] Runtime.UserCodeSyntaxError: Syntax error in module 'lambda_function': invalid syntax (lambda_function.py, line 85) Traceback (most recent call last):   File "/var/task/lambda_function.py" Line 85                 logGroupName=/aws/lambda/sk-func-amazonsales-capstone,

2024-02-21T16:31:44.375+05:30	END RequestId: 61b8efe2-766b-402c-acdc-31514834c3f5

2024-02-21T16:31:44.375+05:30	REPORT RequestId: 61b8efe2-766b-402c-acdc-31514834c3f5 Duration: 1.91 ms Billed Duration: 2 ms Memory Size: 128 MB Max Memory Used: 35 MB

---------------------------------------------------------------------------------------------------------
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:GetLogEvents",
                "logs:FilterLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:*:*:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::amazonsales-capstone-sk/cleanedfiles/*",
                "arn:aws:s3:::amazonsales-capstone-sk/transformed/*"
            ]
        }
    ]
}

===============================================================================
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": [
				"s3:PutObject",
				"s3:GetObject",
				"s3:ListBucket"
			],
			"Resource": [
				"arn:aws:s3:::amazonsales-capstone-sk/cleanedfiles/*",
				"arn:aws:s3:::amazonsales-capstone-sk/transformed/*"
			]
		}
	]
}

---------------------------------------------------------------
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:GetLogEvents",
                "logs:FilterLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:*:*:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::YOUR_BUCKET_NAME/*"
            ]
        }
    ]
}


==============================================================================
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime, timedelta

s3 = boto3.client('s3')
logs = boto3.client('logs')

def lambda_handler(event, context):
    bucket_name = "amazonsales-capstone-sk"
    log_group = '/aws/lambda/sk-func-amazonsales-capstone'
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

def trigger_glue_job(bucket_name, object_key):
    # Check if the file is a parquet and in the cleanedfiles folder
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.parquet'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazon-sales-gluejob-sk"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started successfully.")

        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")

    else:
        print("File is not in the cleanedfiles folder or not a parquet")

    # Get logs from CloudWatch Logs
    try:
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


--------------------------------------------------------------------
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
        log_group = '/aws/lambda/sk-func-amazonsales-capstone'  
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


























