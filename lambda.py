{
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-west-2",
      "eventTime": "2022-02-06T00:00:00.000Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "EXAMPLE"
      },
      "requestParameters": {
        "sourceIPAddress": "127.0.0.1"
      },
      "responseElements": {
        "x-amz-request-id": "EXAMPLE123456789",
        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "testConfigRule",
        "bucket": {
          "name": "amazonsales-capstone-sk",
          "ownerIdentity": {
            "principalId": "EXAMPLE"
          },
          "arn": "arn:aws:s3:::amazonsales-capstone-sk"
        },
        "object": {
          "key": "amazon.csv",
          "size": 1024,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}



























import json
import urllib.parse
import boto3
import pandas as pd
from io import StringIO

print('Loading function')

s3 = boto3.client('s3')

def is_csv(file_path):
    return file_path.lower().endswith('.csv')

def has_required_columns(df):
    required_columns = ['product_id', 'product_name', 'category', 'discounted_price', 'actual_price', 'discount_percentage', 'rating', 'rating_count', 'about_product', 'user_id', 'user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link']
    return all(col in df.columns for col in required_columns)

def standardize_data(df):
    # Cleaning steps
    df['discounted_price'] = df['discounted_price'].str.replace('₹', '').astype(float)
    df['actual_price'] = df['actual_price'].str.replace('₹', '').astype(float)
    df['discount_percentage'] = df['discount_percentage'].str.rstrip('%').astype(float)
    df['rating'] = df['rating'].astype(float)
    return df

def lambda_handler(event, context):
    bucket = 'amazonsales-capstone-sk'  # Replace with your bucket name
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(response['Body'])
        
        if is_csv(key) and has_required_columns(df):
            cleaned_df = standardize_data(df)
            
            # Convert DataFrame to CSV string
            cleaned_csv = cleaned_df.to_csv(index=False)
            
            # Upload the cleaned CSV to a new location
            cleaned_key = 'cleaned/' + key.split('/')[-1]
            s3.put_object(Bucket=bucket, Key=cleaned_key, Body=cleaned_csv)
            
            return {
                'statusCode': 200,
                'body': 'Data cleaned and uploaded successfully!'
            }
        else:
            error_message = 'Invalid CSV format or missing required columns.'
            print(error_message)
            return {
                'statusCode': 400,
                'body': error_message
            }
    except Exception as e:
        error_message = str(e)
        print(error_message)
        return {
            'statusCode': 500,
            'body': error_message
        }
