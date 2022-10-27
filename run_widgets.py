import boto3, json
import sys
from boto3 import client
from botocore.exceptions import ClientError

def check_items():

    conn = client('s3')  # again assumes boto.cfg setup, assume AWS S3
    for key in conn.list_objects(Bucket='usu-cs5260-ignite-dist')['Contents']:
        print(key['Key'])

def put_s3():
    
    DDB = boto3.client('s3', region_name='us-east-1')
    
    try:
        response = DDB.put_item(
            TableName='<FMI_1>',
            Item={
                'product_name': {
                    'S': '<FMI_2>'
                },
                'product_id': {
                    'S': '<FMI_3>'
                },
                'price_in_cents':{
                    'N': '<FMI_4>' #number passed in as a string (ie in quotes)
                },
                'description':{
                    'S': "<FMI_5>"
                },
                'tags':{
                    'L': [{
                            'S': '<FMI_6>'
                        },{
                            'S': '<FMI_7>'
                        }]
                }
            },
            ConditionExpression='attribute_not_exists(product_name)'
        )
    except ClientError as e:
        # Ignore the ConditionalCheckFailedException, bubble up
        # other exceptions.
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            pass

def put_dynamodb():
    
    DDB = boto3.client('dynamodb', region_name='us-east-1')
    
    try:
        response = DDB.put_item(
            TableName='widgets',
            Item={
                'product_name': {
                    'S': '<FMI_2>'
                },
                'product_id': {
                    'S': '<FMI_3>'
                },
                'price_in_cents':{
                    'N': '<FMI_4>' #number passed in as a string (ie in quotes)
                },
                'description':{
                    'S': "<FMI_5>"
                },
                'tags':{
                    'L': [{
                            'S': '<FMI_6>'
                        },{
                            'S': '<FMI_7>'
                        }]
                }
            },
            ConditionExpression='attribute_not_exists(product_name)'
        )
    except ClientError as e:
        # Ignore the ConditionalCheckFailedException, bubble up
        # other exceptions.
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            pass
        
session = boto3.Session()
s3_client = session.client('s3')
b = s3_client.list_buckets()
for item in b['Buckets']:
    print(item['Name'])
    
# try:
if sys.argv[1] != 's3' and sys.argv[1] != 'dynamodb':
    print("Please choose 's3' or 'dynamodb' to choose a location to store widgets")
else:
    print('type "end" to end the program')
    check_items()
        
# except:
#     print("Please choose 's3' or 'dynamodb' to choose a location to store widgets")

# input1 = input()
# print(input1)