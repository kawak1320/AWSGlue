import json
import logging # manda logs al clouldwatch service
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

destination_bucket = 'devopslatam02-datacatalog-mpavesio'
destination_folder = 'results'

athena_staging_table = 'books'
athena_staging_database = 'devopslatam02-db-mpavesio'
athena_s3_results_folder = 'results'

#target_folder_names = [year, month, day]
def create_data_file(target_folder_names):
    # Enviar query a Athena
    query = f"SELECT * FROM {athena_staging_table}"

    target_folder_path = '/'.join(target_folder_names)
    target_s3_folder = f"s3://{destination_bucket}/{athena_s3_results_folder}/{target_folder_path}/"

    athena_client = boto3.client('athena')
    # start_query_execution se ejecuta asyncronicamente
    athena_response = athena_client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = {
            'Database': athena_staging_database
        },
        ResultConfiguration = {
            'OutputLocation': target_s3_folder
        }
    )

    # mirar el estado ejecucion del query para ver si la data esta lista
    execution_id = athena_response['QueryExecutionId']
    query_execution_status = athena_client.get_query_execution(QueryExecutionId=execution_id) # preguntamos cual es el estado de la ejecucion del query
    query_status = query_execution_status['QueryExecution']['Status']['State']
    logger.info(f"query_status: {query_status}")

    # Polling the query state
    while query_status == 'QUEUED' or query_status == 'RUNNING':
        query_execution_status = athena_client.get_query_execution(QueryExecutionId=execution_id) # preguntamos cual es el estado de la ejecucion del query
        query_status = query_execution_status['QueryExecution']['Status']['State']
        logger.info(f"query_execution_status: {query_execution_status} query_status: {query_status}")

    output_filename = query_execution_status['QueryExecution']['ResultConfiguration']['OutputLocation']
    logger.info(f"query: {query}, execution_id: {execution_id}, query_status: {query_status}, output_filename: {output_filename}")


    # escribir resultado en `destination_s3_bucket`/destination_folder/year/month/day
    # boto3.resource nos permite usar objetos: archivos en s3
    s3_resurce= boto3.resource('s3')
        # copiar output_filename en destination_s3_bucket 's3://devopslatam02-result/athena_results/2022/05/01/4557fef1-d80d-4e8f-b05d-aec6834df6ba.csv'

    output_filename_sin_s3 = output_filename.replace('s3://', '')
    tokens = output_filename_sin_s3.split('/')

    # TODO: improve
    source_bucket = tokens[0]
    file_name = tokens[-1]
    source_key = output_filename.replace(f"s3://{source_bucket}", '')

    # athena_results/2022/05/01/4557fef1-d80d-4e8f-b05d-aec6834df6ba.csv --> publications/2022/05/01/
    target_key = source_key.replace(athena_s3_results_folder, destination_folder)
    # .replace(file_name, '')

    logger.info(f"trying to copy {source_bucket} {source_key} into {destination_bucket} {target_key}")

    # quitar el primer / en target y source key target_key.lstrip('/')
    s3_resurce.meta.client.copy( { 'Bucket': source_bucket, 'Key': source_key.lstrip('/')}, destination_bucket, target_key.lstrip('/') )

def create_s3_folder(parent_folders, sub_folder_name):

    s3_client = boto3.client('s3')

    parent_path = '/'.join(parent_folders) # elemento1/elemento2/elementon...
    new_folder_path =  f"{destination_folder}/{sub_folder_name}/"  if parent_path == ''  else  f"{destination_folder}/{parent_path}/{sub_folder_name}/"
    response = s3_client.put_object(Bucket=destination_bucket, Key=new_folder_path)

    http_code = response['ResponseMetadata']['HTTPStatusCode']  # response.ResponseMetadata.HTTPStatusCode
    result = 'SUCCEEDED' if http_code == 200 else 'FAILED'
    logger.info(f'Creating s3 folder {new_folder_path}, status {result}')


# def define una funcion
# lambda_handler es el nombre de la function (entry point como el "main")
# (event, context) son parametros = datos que vienen desde afuera cuando se invoca el lambda
def lambda_handler(event, context):
    logger.info(f"event {event}")
    logger.info(f"context {context}")
    print(f"este mensaje va al default ouput (cloudwatch)")

    # Inspeccionar el event y obtener el “key” (nombre del file que se copio)
    # El `event` puede contener 1 o mas `Records`
    # Cada s3 object tiene un `key` por ejemplo 'key': 'publications_staging/publications_20220501000000.csv'
    # TODO: Loop for each `record` in Records
    uploaded_file_key = event["Records"][0]['s3']['object']['key']

    # publications_20220501000000.csv
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
