import boto3
import botocore
import json
import pymongo
import urllib.parse
import os
import ast
from botocore.exceptions import ClientError

secret_name = os.environ['secret_name']
region_name = os.environ['region']
pem_locator = os.environ['pem_locator']
    
session = boto3.session.Session()
client_secret = session.client(service_name = 'secretsmanager', region_name = region_name)
client_s3 = session.client(service_name = 's3', region_name = region_name, config=botocore.config.Config(s3={'addressing_style':'path'}))
get_secret_value_response = "null"
# get secret
try:
    get_secret_value_response = client_secret.get_secret_value(SecretId = secret_name)
except ClientError as e:
    raise e
	    
secret_data = json.loads(get_secret_value_response['SecretString'])

username = secret_data['username']
password = secret_data['password']
docdb_host = secret_data['host']
docdb_port = str(secret_data['port'])

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
    
    # for test purposes select 3 objects
    #data_three = data[0:3]
     
    testdb = db_client['testdb']
    testcoll = testdb['recipes']
    
    # this is just for test purposes
    #testcoll.drop()

    ##Insert a single document
    testcoll.insert_many(data)
    
    ##Find the document that was previously written
    #x = testcoll.find_one({'title':'Amazon DocumentDB'})
    #for doc in testcoll.find():
        #print(doc)

    number = testcoll.find().count()
    
    print(number)
    ##Close the connection
    db_client.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }