import boto3

def lambda_handler(event, context):
    # Get the bucket name and the object key from the event
    bucket_name = "amazonsales-capstone-sk"
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Check if the file is a CSV and in the cleanedfiles folder
    if object_key.startswith('cleanedfiles/') and object_key.endswith('.csv'):
        try:
            # Trigger the Glue job
            glue = boto3.client('glue')
            job_name = "amazonsales-sk-job"
            response = glue.start_job_run(JobName=job_name)
            print("Glue job started:", response)
        except Exception as e:
            print(f"An error occurred while triggering the Glue job: {str(e)}")
    else:
        print("File is not in the cleanedfiles folder or not a CSV")
