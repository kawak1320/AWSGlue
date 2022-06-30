import json
import logging 
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

destination_bucket = 'devopslatam02-datacatalog-mpavesio'
destination_folder = 'results'

athena_staging_table = 'books'
athena_staging_database = 'devopslatam02-db-mpavesio'
athena_s3_results_folder = 'results'

def create_data_file(target_folder_names):
    query = f"SELECT * FROM {athena_staging_table}"

    target_folder_path = '/'.join(target_folder_names)
    target_s3_folder = f"s3://{destination_bucket}/{athena_s3_results_folder}/{target_folder_path}/"

    athena_client = boto3.client('athena')
    athena_response = athena_client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = {
            'Database': athena_staging_database
        },
        ResultConfiguration = {
            'OutputLocation': target_s3_folder
        }
    )

    execution_id = athena_response['QueryExecutionId']
    query_execution_status = athena_client.get_query_execution(QueryExecutionId=execution_id) 
    query_status = query_execution_status['QueryExecution']['Status']['State']
    logger.info(f"query_status: {query_status}")

    while query_status == 'QUEUED' or query_status == 'RUNNING':
        query_execution_status = athena_client.get_query_execution(QueryExecutionId=execution_id) 
        query_status = query_execution_status['QueryExecution']['Status']['State']
        logger.info(f"query_execution_status: {query_execution_status} query_status: {query_status}")

    output_filename = query_execution_status['QueryExecution']['ResultConfiguration']['OutputLocation']
    logger.info(f"query: {query}, execution_id: {execution_id}, query_status: {query_status}, output_filename: {output_filename}")

    s3_resurce= boto3.resource('s3')

    output_filename_sin_s3 = output_filename.replace('s3://', '')
    tokens = output_filename_sin_s3.split('/')

    source_bucket = tokens[0]
    file_name = tokens[-1]
    source_key = output_filename.replace(f"s3://{source_bucket}", '')

    target_key = source_key.replace(athena_s3_results_folder, destination_folder)

    logger.info(f"trying to copy {source_bucket} {source_key} into {destination_bucket} {target_key}")

    s3_resurce.meta.client.copy( { 'Bucket': source_bucket, 'Key': source_key.lstrip('/')}, destination_bucket, target_key.lstrip('/') )

def create_s3_folder(parent_folders, sub_folder_name):

    s3_client = boto3.client('s3')

    parent_path = '/'.join(parent_folders) 
    new_folder_path =  f"{destination_folder}/{sub_folder_name}/"  if parent_path == ''  else  f"{destination_folder}/{parent_path}/{sub_folder_name}/"
    response = s3_client.put_object(Bucket=destination_bucket, Key=new_folder_path)

    http_code = response['ResponseMetadata']['HTTPStatusCode']  
    result = 'SUCCEEDED' if http_code == 200 else 'FAILED'
    logger.info(f'Creating s3 folder {new_folder_path}, status {result}')

def lambda_handler(event, context):
    logger.info(f"event {event}")
    logger.info(f"context {context}")
    print(f"este mensaje va al default ouput (cloudwatch)")

    uploaded_file_key = event["Records"][0]['s3']['object']['key']

    uploaded_file_name = uploaded_file_key[ uploaded_file_key.find('/') + 1: ]
    logger.info(f"uploaded_file_name {uploaded_file_name}")

    year = uploaded_file_name[uploaded_file_name.find('_')+1:uploaded_file_name.find('_')+5]
    month = uploaded_file_name[uploaded_file_name.find('_')+5:uploaded_file_name.find('_')+7]
    day = uploaded_file_name[uploaded_file_name.find('_')+7:uploaded_file_name.find('_')+9]

    create_s3_folder([], year)
    create_s3_folder([year], month)
    create_s3_folder([year, month], day)

    target_folder_names = [year, month, day]
    create_data_file(target_folder_names)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
