import json
import boto3
import time
import math

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

# Define your S3 bucket and key to store the JobRunId
s3_bucket = 'aws-mysql-to-sf-lambda-dev-us-west-2'
s3_key = 'glue-job-status/alldata_job_run_id.json'

def get_job_status():
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        job_status = json.loads(response['Body'].read().decode('utf-8'))
        return job_status
    except s3_client.exceptions.NoSuchKey:
        return None
    except s3_client.exceptions.ClientError as e:
        print(f"Error getting job status: {e}")
        raise

def save_job_status(job_status):
    try:
        s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=json.dumps(job_status))
    except s3_client.exceptions.ClientError as e:
        print(f"Error saving job status: {e}")
        raise

def lambda_handler(event, context):
    job_name = 'glue-mysql-to-sf-snowflake-etl-dev-alldata'
    
    job_status = get_job_status()
    
    if not job_status or 'JobRunId' not in job_status:
        try:
            response = glue_client.start_job_run(JobName=job_name)
            job_run_id = response['JobRunId']
            save_job_status({'JobRunId': job_run_id, 'Status': 'STARTED'})
            return {
                'statusCode': 200,
                'JobRunId': job_run_id,
                'Status': 'STARTED'
            }
        except glue_client.exceptions.ConcurrentRunsExceededException as e:
            print(f"ConcurrentRunsExceededException: {e}")
            # Retry with exponential backoff
            max_retries = 5
            base_delay = 2  # seconds
            for retry in range(max_retries):
                delay = base_delay * math.pow(2, retry)
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                try:
                    response = glue_client.start_job_run(JobName=job_name)
                    job_run_id = response['JobRunId']
                    save_job_status({'JobRunId': job_run_id, 'Status': 'STARTED'})
                    return {
                        'statusCode': 200,
                        'JobRunId': job_run_id,
                        'Status': 'STARTED'
                    }
                except glue_client.exceptions.ConcurrentRunsExceededException as e:
                    print(f"ConcurrentRunsExceededException on retry {retry + 1}: {e}")
            else:
                # Max retries exceeded, handle the error or return a specific response
                print(f"Max retries exceeded. Unable to start job.")
                return {
                    'statusCode': 500,
                    'message': 'Max retries exceeded. Unable to start job.'
                }
    else:
        job_run_id = job_status['JobRunId']
        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        current_status = response['JobRun']['JobRunState']
        
        if current_status in ['SUCCEEDED', 'FAILED']:
            time.sleep(600)  # Wait for 5 minutes
            try:
                new_response = glue_client.start_job_run(JobName=job_name)
                new_job_run_id = new_response['JobRunId']
                save_job_status({'JobRunId': new_job_run_id, 'Status': 'STARTED'})
                return {
                    'statusCode': 200,
                    'JobRunId': new_job_run_id,
                    'Status': 'STARTED'
                }
            except glue_client.exceptions.ConcurrentRunsExceededException as e:
                print(f"ConcurrentRunsExceededException: {e}")
                # Retry with exponential backoff
                max_retries = 5
                base_delay = 2  # seconds
                for retry in range(max_retries):
                    delay = base_delay * math.pow(2, retry)
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    try:
                        response = glue_client.start_job_run(JobName=job_name)
                        new_job_run_id = response['JobRunId']
                        save_job_status({'JobRunId': new_job_run_id, 'Status': 'STARTED'})
                        return {
                            'statusCode': 200,
                            'JobRunId': new_job_run_id,
                            'Status': 'STARTED'
                        }
                    except glue_client.exceptions.ConcurrentRunsExceededException as e:
                        print(f"ConcurrentRunsExceededException on retry {retry + 1}: {e}")
                else:
                    # Max retries exceeded, handle the error or return a specific response
                    print(f"Max retries exceeded. Unable to start job.")
                    return {
                        'statusCode': 500,
                        'message': 'Max retries exceeded. Unable to start job.'
                    }
        else:
            return {
                'statusCode': 200,
                'JobRunId': job_run_id,
                'Status': current_status
            }
