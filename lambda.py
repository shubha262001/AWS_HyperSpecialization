Details: "ADO.NET: Python script error.
<pi>  File "PythonScriptWrapper.PY", line 13
    pip install pandas
        ^
SyntaxError: invalid syntax
</pi>"

-----------------------------------------
Microsoft Windows [Version 10.0.14393]
(c) 2016 Microsoft Corporation. All rights reserved.

C:\Users\shubha.k>cd
C:\Users\shubha.k

C:\Users\shubha.k>C:\Program Files\Python38
'C:\Program' is not recognized as an internal or external command,
operable program or batch file.

C:\Users\shubha.k>C:\Program Files\Python38
'C:\Program' is not recognized as an internal or external command,
operable program or batch file.

C:\Users\shubha.k>cd C:\Program Files\Python38

C:\Program Files\Python38>python -m pip install pandas
Requirement already satisfied: pandas in c:\users\shubha.k\appdata\roaming\python\python38\site-packages (2.0.3)
Requirement already satisfied: tzdata>=2022.1 in c:\users\shubha.k\appdata\roaming\python\python38\site-packages (from pandas) (2024.1)
Requirement already satisfied: numpy>=1.20.3; python_version < "3.10" in c:\users\shubha.k\appdata\roaming\python\python38\site-packages (from pandas) (1.24.4)
Requirement already satisfied: pytz>=2020.1 in c:\users\shubha.k\appdata\roaming\python\python38\site-packages (from pandas) (2024.1)
Requirement already satisfied: python-dateutil>=2.8.2 in c:\users\shubha.k\appdata\roaming\python\python38\site-packages (from pandas) (2.8.2)
Requirement already satisfied: six>=1.5 in c:\users\shubha.k\appdata\roaming\python\python38\site-packages (from python-dateutil>=2.8.2->pandas) (1.16.0)
WARNING: You are using pip version 19.2.3, however version 24.0 is available.
You should consider upgrading via the 'python -m pip install --upgrade pip' command.

C:\Program Files\Python38>python -m pip install --user boto3 pandas
Collecting boto3
  Downloading https://files.pythonhosted.org/packages/a8/ec/5cf74e265bb3fc764cf475cd6208e4d189d4e7938c39f7e499894fa5054d/boto3-1.34.49-py3-none-any.whl (139kB)
     |████████████████████████████████| 143kB 3.2MB/s
Requirement already satisfied: pandas in c:\users\shubha.k\appdata\roaming\python\python38\site-packages (2.0.3)
Collecting botocore<1.35.0,>=1.34.49 (from boto3)
  Downloading https://files.pythonhosted.org/packages/bd/e3/6bb6c8abea700d5dfeb14bbe41bdfe27e0aa665d06a911397a51e57aaa69/botocore-1.34.49-py3-none-any.whl (12.0MB)
     |████████████████████████████████| 12.0MB 6.4MB/s
Collecting jmespath<2.0.0,>=0.7.1 (from boto3)
  Downloading https://files.pythonhosted.org/packages/31/b4/b9b800c45527aadd64d5b442f9b932b00648617eb5d63d2c7a6587b7cafc/jmespath-1.0.1-py3-none-any.whl
Collecting s3transfer<0.11.0,>=0.10.0 (from boto3)
  Downloading https://files.pythonhosted.org/packages/12/bb/7e7912e18cd558e7880d9b58ffc57300b2c28ffba9882b3a54ba5ce3ebc4/s3transfer-0.10.0-py3-none-any.whl (82kB)
     |████████████████████████████████| 92kB 3.0MB/s
Requirement already satisfied: tzdata>=2022.1 in c:\users\shubha.k\appdata\roaming\python\python38\site-packages (from pandas) (2024.1)
Requirement already satisfied: python-dateutil>=2.8.2 in c:\users\shubha.k\appdata\roaming\python\python38\site-packages (from pandas) (2.8.2)
Requirement already satisfied: numpy>=1.20.3; python_version < "3.10" in c:\users\shubha.k\appdata\roaming\python\python38\site-packages (from pandas) (1.24.4)
Requirement already satisfied: pytz>=2020.1 in c:\users\shubha.k\appdata\roaming\python\python38\site-packages (from pandas) (2024.1)
Collecting urllib3<1.27,>=1.25.4; python_version < "3.10" (from botocore<1.35.0,>=1.34.49->boto3)
  Downloading https://files.pythonhosted.org/packages/b0/53/aa91e163dcfd1e5b82d8a890ecf13314e3e149c05270cc644581f77f17fd/urllib3-1.26.18-py2.py3-none-any.whl (143kB)
     |████████████████████████████████| 153kB 6.8MB/s
Requirement already satisfied: six>=1.5 in c:\users\shubha.k\appdata\roaming\python\python38\site-packages (from python-dateutil>=2.8.2->pandas) (1.16.0)
Installing collected packages: jmespath, urllib3, botocore, s3transfer, boto3
Successfully installed boto3-1.34.49 botocore-1.34.49 jmespath-1.0.1 s3transfer-0.10.0 urllib3-1.26.18
WARNING: You are using pip version 19.2.3, however version 24.0 is available.
You should consider upgrading via the 'python -m pip install --upgrade pip' command.

C:\Program Files\Python38>
-----------------------------------------------------------------------------------
-C:\Program Files\Python38>python -m pip install --user pandas
Collecting pandas
  Using cached https://files.pythonhosted.org/packages/c3/6c/ea362eef61f05553aaf1a24b3e96b2d0603f5dc71a3bd35688a24ed88843/pandas-2.0.3-cp38-cp38-win_amd64.whl
Collecting python-dateutil>=2.8.2 (from pandas)
  Using cached https://files.pythonhosted.org/packages/36/7a/87837f39d0296e723bb9b62bbb257d0355c7f6128853c78955f57342a56d/python_dateutil-2.8.2-py2.py3-none-any.whl
Collecting numpy>=1.20.3; python_version < "3.10" (from pandas)
  Using cached https://files.pythonhosted.org/packages/69/65/0d47953afa0ad569d12de5f65d964321c208492064c38fe3b0b9744f8d44/numpy-1.24.4-cp38-cp38-win_amd64.whl
Collecting pytz>=2020.1 (from pandas)
  Using cached https://files.pythonhosted.org/packages/9c/3d/a121f284241f08268b21359bd425f7d4825cffc5ac5cd0e1b3d82ffd2b10/pytz-2024.1-py2.py3-none-any.whl
Collecting tzdata>=2022.1 (from pandas)
  Using cached https://files.pythonhosted.org/packages/65/58/f9c9e6be752e9fcb8b6a0ee9fb87e6e7a1f6bcab2cdc73f02bb7ba91ada0/tzdata-2024.1-py2.py3-none-any.whl
Collecting six>=1.5 (from python-dateutil>=2.8.2->pandas)
  Using cached https://files.pythonhosted.org/packages/d9/5a/e7c31adbe875f2abbb91bd84cf2dc52d792b5a01506781dbcf25c91daf11/six-1.16.0-py2.py3-none-any.whl
Installing collected packages: six, python-dateutil, numpy, pytz, tzdata, pandas
  WARNING: The script f2py.exe is installed in 'C:\Users\shubha.k\AppData\Roaming\Python\Python38\Scripts' which is not on PATH.
  Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.
Successfully installed numpy-1.24.4 pandas-2.0.3 python-dateutil-2.8.2 pytz-2024.1 six-1.16.0 tzdata-2024.1
WARNING: You are using pip version 19.2.3, however version 24.0 is available.
You should consider upgrading via the 'python -m pip install --upgrade pip' command.

C:\Program Files\Python38>
C:\Program Files\Python38>python -m venu  myenv
C:\Program Files\Python38\python.exe: No module named venu

C:\Program Files\Python38>

----------------------------------------------------------------------------
Microsoft Windows [Version 10.0.14393]
(c) 2016 Microsoft Corporation. All rights reserved.

C:\Users\shubha.k>cd
C:\Users\shubha.k

C:\Users\shubha.k>cd C:\Program Files\Python38

C:\Program Files\Python38>python -m pip install pandas
Collecting pandas
  Downloading https://files.pythonhosted.org/packages/c3/6c/ea362eef61f05553aaf1a24b3e96b2d0603f5dc71a3bd35688a24ed88843/pandas-2.0.3-cp38-cp38-win_amd64.whl (10.8MB)
     |████████████████████████████████| 10.8MB 6.4MB/s
Collecting python-dateutil>=2.8.2 (from pandas)
  Downloading https://files.pythonhosted.org/packages/36/7a/87837f39d0296e723bb9b62bbb257d0355c7f6128853c78955f57342a56d/python_dateutil-2.8.2-py2.py3-none-any.whl (247kB)
     |████████████████████████████████| 256kB 6.8MB/s
Collecting tzdata>=2022.1 (from pandas)
  Downloading https://files.pythonhosted.org/packages/65/58/f9c9e6be752e9fcb8b6a0ee9fb87e6e7a1f6bcab2cdc73f02bb7ba91ada0/tzdata-2024.1-py2.py3-none-any.whl (345kB)
     |████████████████████████████████| 348kB 6.8MB/s
Collecting numpy>=1.20.3; python_version < "3.10" (from pandas)
  Downloading https://files.pythonhosted.org/packages/69/65/0d47953afa0ad569d12de5f65d964321c208492064c38fe3b0b9744f8d44/numpy-1.24.4-cp38-cp38-win_amd64.whl (14.9MB)
     |████████████████████████████████| 14.9MB 6.4MB/s
Collecting pytz>=2020.1 (from pandas)
  Downloading https://files.pythonhosted.org/packages/9c/3d/a121f284241f08268b21359bd425f7d4825cffc5ac5cd0e1b3d82ffd2b10/pytz-2024.1-py2.py3-none-any.whl (505kB)
     |████████████████████████████████| 512kB ...
Collecting six>=1.5 (from python-dateutil>=2.8.2->pandas)
  Downloading https://files.pythonhosted.org/packages/d9/5a/e7c31adbe875f2abbb91bd84cf2dc52d792b5a01506781dbcf25c91daf11/six-1.16.0-py2.py3-none-any.whl
Installing collected packages: six, python-dateutil, tzdata, numpy, pytz, pandas
ERROR: Could not install packages due to an EnvironmentError: [Errno 13] Permission denied: 'C:\\Program Files\\Python38\\Lib\\site-packages\\six.py'
Consider using the `--user` option or check the permissions.

WARNING: You are using pip version 19.2.3, however version 24.0 is available.
You should consider upgrading via the 'python -m pip install --upgrade pip' command.

C:\Program Files\Python38>pip install pandas
Collecting pandas
  Using cached https://files.pythonhosted.org/packages/c3/6c/ea362eef61f05553aaf1a24b3e96b2d0603f5dc71a3bd35688a24ed88843/pandas-2.0.3-cp38-cp38-win_amd64.whl
Collecting python-dateutil>=2.8.2 (from pandas)
  Using cached https://files.pythonhosted.org/packages/36/7a/87837f39d0296e723bb9b62bbb257d0355c7f6128853c78955f57342a56d/python_dateutil-2.8.2-py2.py3-none-any.whl
Collecting numpy>=1.20.3; python_version < "3.10" (from pandas)
  Using cached https://files.pythonhosted.org/packages/69/65/0d47953afa0ad569d12de5f65d964321c208492064c38fe3b0b9744f8d44/numpy-1.24.4-cp38-cp38-win_amd64.whl
Collecting pytz>=2020.1 (from pandas)
  Using cached https://files.pythonhosted.org/packages/9c/3d/a121f284241f08268b21359bd425f7d4825cffc5ac5cd0e1b3d82ffd2b10/pytz-2024.1-py2.py3-none-any.whl
Collecting tzdata>=2022.1 (from pandas)
  Using cached https://files.pythonhosted.org/packages/65/58/f9c9e6be752e9fcb8b6a0ee9fb87e6e7a1f6bcab2cdc73f02bb7ba91ada0/tzdata-2024.1-py2.py3-none-any.whl
Collecting six>=1.5 (from python-dateutil>=2.8.2->pandas)
  Using cached https://files.pythonhosted.org/packages/d9/5a/e7c31adbe875f2abbb91bd84cf2dc52d792b5a01506781dbcf25c91daf11/six-1.16.0-py2.py3-none-any.whl
Installing collected packages: six, python-dateutil, numpy, pytz, tzdata, pandas
ERROR: Could not install packages due to an EnvironmentError: [Errno 13] Permission denied: 'c:\\program files\\python38\\Lib\\site-packages\\six.py'
Consider using the `--user` option or check the permissions.

WARNING: You are using pip version 19.2.3, however version 24.0 is available.
You should consider upgrading via the 'python -m pip install --upgrade pip' command.

C:\Program Files\Python38>
-----------------------------------------------------
Details: "ADO.NET: Python script error.
<pi>  File "PythonScriptWrapper.PY", line 13
    pip install pandas
        ^
SyntaxError: invalid syntax
</pi>"
----------------------------------------------
pip install pandas
import pandas as pd
import boto3
from io import BytesIO

# Define AWS credentials
aws_access_key_id = 'AKIA5BUWPDK3VBKTB27C'
aws_secret_access_key = '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'
bucket_name = 'amazonsales-capstone-sk'
folder = 'transformed'  # specify the folder if the files are inside a folder

# Connect to S3
s3 = boto3.client('s3', 
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)

# List objects in the folder
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{folder}/")

# Read Parquet files into a pandas DataFrame
dfs = []
for obj in response['Contents']:
    if obj['Key'].endswith('.parquet'):
        file_name = obj['Key']
        obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        dfs.append(df)

# Concatenate all DataFrames
result_df = pd.concat(dfs)

-----------------------------------------------------------------------------------------
@timestamp=2024-02-21 15:41:20.628
@message=REPORT RequestId: 0280709d-1b0a-4aa5-86e9-0970d7d39e0b	Duration: 11108.09 ms	Billed Duration: 11109 ms	Memory Size: 128 MB	Max Memory Used: 129 MB	
@ptr=CnYKOQo1ODk2ODg5OTg1NzE5Oi9hd3MvbGFtYmRhL3NrLWZ1bmMtYW1hem9uc2FsZXMtY2Fwc3RvbmUQABI1GhgCBlmy+KcAAAABOBGDFgAGXWGWIAAAAnIgASjRqtDi3DEw9LbQ4twxOANAxwJIhwxQsQYYACABEAIYAQ==
----------------------------------------------------------------------
completely sucesss;;;;
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
from datetime import datetime, timedelta

s3 = boto3.client('s3')
client = boto3.client('logs')

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
        
    # Export CloudWatch Logs to S3
    query = """fields @timestamp, @message | sort @timestamp desc | LIMIT 1"""
    log_group = '/aws/lambda/sk-func-amazonsales-capstone'
    s3_key = "cloudwatchlogs/{}.txt".format(datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
    
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

    log_data = ""
    for result in response['results']:
        for field in result:
            log_data += f"{field['field']}={field['value']}\n"

    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=log_data)

    return {
        'statusCode': 200,
        'body': "Success"
    }

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



-------------------------------------------------------------------------------------------------------------------
[ERROR] JSONDecodeError: Invalid \escape: line 1 column 1076 (char 1075)
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 89, in lambda_handler
    json_object = json.loads(response_string)
  File "/var/lang/lib/python3.12/json/__init__.py", line 346, in loads
    return _default_decoder.decode(s)
  File "/var/lang/lib/python3.12/json/decoder.py", line 337, in decode
    obj, end = self.raw_decode(s, idx=_w(s, 0).end())
  File "/var/lang/lib/python3.12/json/decoder.py", line 353, in raw_decode
    obj, end = self.scan_once(s, idx)

----------------------------------------------------------------------------------
Timestamp
Message
No older events at this moment. 
Retry

2024-02-21T20:57:12.828+05:30	INIT_START Runtime Version: python:3.12.v18 Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:776a3759221679a634181f858871d5514dc74a176f78bc535f822a932845ae5a

2024-02-21T20:57:15.955+05:30	START RequestId: 838e0682-f067-4045-b63f-42f934412760 Version: $LATEST

2024-02-21T20:57:16.701+05:30	An error occurred: An error occurred (NoSuchBucket) when calling the GetObject operation: The specified bucket does not exist

2024-02-21T20:57:16.818+05:30
[ERROR] NoSuchBucket: An error occurred (NoSuchBucket) when calling the CopyObject operation: The specified bucket does not exist
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 59, in lambda_handler
    s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})
  File "/var/lang/lib/python3.12/site-packages/botocore/client.py", line 535, in _api_call
    return self._make_api_call(operation_name, kwargs)
  File "/var/lang/lib/python3.12/site-packages/botocore/client.py", line 980, in _make_api_call
    raise error_class(parsed_response, operation_name)

Copy
[ERROR] NoSuchBucket: An error occurred (NoSuchBucket) when calling the CopyObject operation: The specified bucket does not exist Traceback (most recent call last):   File "/var/task/lambda_function.py", line 59, in lambda_handler     s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})   File "/var/lang/lib/python3.12/site-packages/botocore/client.py", line 535, in _api_call     return self._make_api_call(operation_name, kwargs)   File "/var/lang/lib/python3.12/site-packages/botocore/client.py", line 980, in _make_api_call     raise error_class(parsed_response, operation_name)

2024-02-21T20:57:16.839+05:30	END RequestId: 838e0682-f067-4045-b63f-42f934412760

2024-02-21T20:57:16.839+05:30	REPORT RequestId: 838e0682-f067-4045-b63f-42f934412760 Duration: 884.70 ms Billed Duration: 885 ms Memory Size: 128 MB Max Memory Used: 128 MB Init Duration: 3124.80 ms

2024-02-21T20:58:19.977+05:30	START RequestId: 838e0682-f067-4045-b63f-42f934412760 Version: $LATEST

2024-02-21T20:58:20.559+05:30	An error occurred: An error occurred (NoSuchBucket) when calling the GetObject operation: The specified bucket does not exist

2024-02-21T20:58:20.599+05:30
[ERROR] NoSuchBucket: An error occurred (NoSuchBucket) when calling the CopyObject operation: The specified bucket does not exist
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 59, in lambda_handler
    s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})
  File "/var/lang/lib/python3.12/site-packages/botocore/client.py", line 535, in _api_call
    return self._make_api_call(operation_name, kwargs)
  File "/var/lang/lib/python3.12/site-packages/botocore/client.py", line 980, in _make_api_call
    raise error_class(parsed_response, operation_name)

Copy
[ERROR] NoSuchBucket: An error occurred (NoSuchBucket) when calling the CopyObject operation: The specified bucket does not exist Traceback (most recent call last):   File "/var/task/lambda_function.py", line 59, in lambda_handler     s3.copy_object(Bucket=bucket_name, Key=error_object_key, CopySource={'Bucket': bucket_name, 'Key': object_key})   File "/var/lang/lib/python3.12/site-packages/botocore/client.py", line 535, in _api_call     return self._make_api_call(operation_name, kwargs)   File "/var/lang/lib/python3.12/site-packages/botocore/client.py", line 980, in _make_api_call     raise error_class(parsed_response, operation_name)

2024-02-21T20:58:20.619+05:30	END RequestId: 838e0682-f067-4045-b63f-42f934412760

2024-02-21T20:58:20.619+05:30	REPORT RequestId: 838e0682-f067-4045-b63f-42f934412760 Duration: 642.02 ms Billed Duration: 643 ms 

---------------------------------------------------------------
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import json
import time
from datetime import datetime, timedelta

s3 = boto3.client('s3')
client = boto3.client('logs')

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
        
    # Export CloudWatch Logs to S3
    query = """fields @timestamp, @message | sort @timestamp desc | LIMIT 5"""
    log_group = '/aws/lambda/sk-func-amazonsales-capstone'
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
        print("File is not in the cleanedfiles folder or not a par


----------------------------------------------------------------------------------------------
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import json
import time
from datetime import datetime, timedelta

s3 = boto3.client('s3')
client = boto3.client('logs')

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
        
def lambda_handler(event, context):
    
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

--------------------------------------------------------------------------
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


























