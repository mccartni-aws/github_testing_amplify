import boto3
import botocore
import json
import pymongo
import urllib.parse
import os
import ast
import pandas as pd
from botocore.exceptions import ClientError

# get values from environment variables
secret_name = os.environ['secret_name']
region_name = os.environ['region']
pem_locator = os.environ['pem_locator']

# connect to s3 and secretsmanager
session = boto3.session.Session()
client_secret = session.client(service_name = 'secretsmanager', region_name = region_name)
client_s3 = session.client(service_name = 's3', region_name = region_name, config=botocore.config.Config(s3={'addressing_style':'path'}))
get_secret_value_response = "null"

# get secret from secret manager
try:
    get_secret_value_response = client_secret.get_secret_value(SecretId = secret_name)
except ClientError as e:
    raise e

secret_data = json.loads(get_secret_value_response['SecretString'])
username = secret_data['username']
password = secret_data['password']
docdb_host = secret_data['host']
docdb_port = str(secret_data['port'])

# connect to documentDb
db_client = pymongo.MongoClient('mongodb://'+username+':'+password+'@'+docdb_host+':'+docdb_port+'/?ssl=true&ssl_ca_certs='+pem_locator)

def recipes_handler(event, context):
    
    # get file from S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        data = client_s3.get_object(Bucket = bucket, Key = key)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    contents = data['Body'].read()
    data = json.loads(contents)
    
    df = pd.DataFrame(data)
    # drop NaN and remove duplivated recipes
    # duplicated recipes are those with same title and desc
    df.dropna(subset = ['title'], inplace = True)
    df.drop_duplicates(subset = ['title', 'desc'], keep = 'first', inplace = True)
    resulted_data = json.loads(df.to_json(orient = 'records'))
    
    db = db_client['recipesdb']
    collection = db['recipes']

    ##Insert data from file into DocumentDB
    collection.insert_many(resulted_data)
    
    ##Close the connection
    db_client.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Recipes are put into database!')
    }