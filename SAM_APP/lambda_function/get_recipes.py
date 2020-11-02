import nltk
nltk.data.path.append("/tmp")
nltk.download("wordnet", download_dir = "/tmp")
from nltk.corpus import wordnet as wn
import boto3
import botocore
import json
import pymongo
import urllib.parse
import os
import ast
from botocore.exceptions import ClientError
import inflect
from bson.json_util import dumps
import re

def if_food(word):

    syns = wn.synsets(word, pos = wn.NOUN)
    lexnames = [syn.lexname() for syn in syns]
    if 'noun.food' in lexnames and 'noun.artifact' not in lexnames:
        return word

p = inflect.engine()

# get values from environment variables
secret_name = os.environ['secret_name']
region_name = os.environ['region']
pem_locator = os.environ['pem_locator']

# connect to secretsmanager
session = boto3.session.Session()
client_secret = session.client(service_name = 'secretsmanager', region_name = region_name)
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
    try:
            ingredients_string = event['pathParameters']['ingredients']
    except Exception as e:
        return {
            'statusCode':500,
            'body': json.dumps({'Error':'No entered ingredients'})
        }
        
    # will be passed as a parameter
    ingredients = [elem.strip() for elem in ingredients_string.split(',')]
    different_forms = [p.singular_noun(elem) if p.singular_noun(elem) else p.plural(elem) for elem in ingredients] + ingredients
    regex = re.compile('|'.join('\\b{0}\\b'.format(w) for w in different_forms), re.IGNORECASE)
    
    db = db_client['recipesdb']
    recipes = db['recipes']
    
    # select recipes that are represented as a string with multiple ingredients inside
    pipeline_single_recipes =[
        {'$match': {'ingredients': { '$elemMatch': { '$exists': 'true' } }}},
        {'$project': {'ingredients':1, 'elemnum':{'$size':"$ingredients"}}},
        {'$unwind': '$ingredients'},
        {'$match': {"ingredients" : {"$regex": regex, "$options" : "i"}}},
        {'$match': {"elemnum": {'$eq': 1}}},
        {'$project': {'ingredients':1}},
        {'$out' : "single_recipes" }
    ]
    recipes.aggregate(pipeline_single_recipes)
    
 
    # find food from single recipes and calculate the difference
    single_recipes = list(db['single_recipes'].find())
    if len(single_recipes):
        selected_single = []
        for recipe in single_recipes:
            newdict = {}
            newdict['_id'] = recipe['_id']
            ingrediends = re.findall('[^\s,;.()<>]+', recipe['ingredients'])
            foundfood = [if_food(ingr) for ingr in ingrediends]
            food = list(filter(None.__ne__, foundfood))
            finded_ingr = [foo for foo in food if re.match(regex,foo)]
            newdict['ingrDifference'] = len(food) - len(finded_ingr)
            selected_single.append(newdict)
        
    pipeline_selected_recipes = [
        {'$match': {'ingredients': { '$elemMatch': { '$exists': 'true' } }}},
        {'$project': {'ingredients':1, 'elemnum':{'$size':"$ingredients"}}},
        {'$unwind': '$ingredients'},
        {'$match': {"ingredients" : {"$regex": regex, "$options" : "i"}}},
        {'$match': {"elemnum": {'$gt': 1}}},
        { "$group": {
            "_id": {
                "_id": "$_id",
                "elemnum": "$elemnum"
            },
           "matchingr": { "$sum": 1 }
        }},
        {'$project': {
            '_id': "$_id._id",
            'ingrDifference': { '$subtract': [ "$_id.elemnum", "$matchingr" ] }
        }},
        { '$out' : "selected_recipes" }
    ]
    recipes.aggregate(pipeline_selected_recipes)
    
    # add single_recipes (if exists) to all selected recipes
    if len(single_recipes):
        db['selected_recipes'].insert_many(selected_single)
    
    # if there are recipes that contains at least one ingredient,
    # then perform the following actions
    # sort and select top 5
    if len(list(db['selected_recipes'].find())):
        pipeline_sort = [
            {'$sort' : { 'ingrDifference' : 1 } },
            {'$limit': 5},
            { '$out' : "selected_ids" }
        ]
        db['selected_recipes'].aggregate(pipeline_sort)
        
        # perform join   
        pipeline_joined = [
           {
             '$lookup':
               {
                 'from': "recipes",
                 'localField': "_id",
                 'foreignField': "_id",
                 'as': "joined_collection"
               }
            },
            {
               "$unwind": "$joined_collection"
            },
            {
                "$addFields": {
                      'title': '$joined_collection.title',
                      'directions': '$joined_collection.directions',
                      'calories': '$joined_collection.calories',
                      'ingredients': '$joined_collection.ingredients',
                   }
            },
            {
                "$project": {
                      'title': 1,
                      'directions': 1,
                      'calories': 1,
                      'ingredients': 1,
                      '_id':0
                   }        
            },
            { '$out' : "results" }
        ]
        db['selected_ids'].aggregate(pipeline_joined)
    
        result_recipes = list(db['results'].find())
        
            
        # create a readable output
        for recipe in result_recipes:
            recipe['directions'] = '\n'.join(recipe['directions'])
            recipe['ingredients'] = ';\n'.join(recipe['ingredients'])
            recipe.pop('_id')
        
            
        #drop used collections
        db['selected_ids'].drop()
        db['selected_recipes'].drop()
        db['single_recipes'].drop()
        db['results'].drop()
    
            
        return {
            'statusCode':200,
            'body': dumps(result_recipes)
        }
    
    else:
        
        #drop used collections
        db['selected_ids'].drop()
        db['selected_recipes'].drop()
        db['single_recipes'].drop()
        db['results'].drop()

        return {
            'statusCode':200,
            'body': 'Currently the database does not have recipes containing entered ingredients'
        }

        
    ##Close the connection
    db_client.close()
