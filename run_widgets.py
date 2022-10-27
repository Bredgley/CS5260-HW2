import boto3, json
import sys
from boto3 import client
from botocore.exceptions import ClientError

def check_items():
    wait_end = ""
    bucket = 'usu-cs5260-ignite-requests'
    key = 'hello world.txt'
    
    # while wait_end != "end"
    conn = client('s3')  
    for key in conn.list_objects(Bucket=bucket)['Contents']:
        # print(key['Key'])
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket, Key=key['Key'])
        j_data = json.loads(obj['Body'].read())
        create_widget_s3(j_data)
        

def create_widget_s3(j_data):
    s3 = boto3.client('s3')
    
    # try:
    new_object={
        'widget_id': {
            'S': j_data['widgetId']
        },
        'owner': {
            'S': j_data['owner']
        },
        'label':{
            'S': j_data['label']
        },
        'description':{
            'S': j_data['description']
        }
    }
    
    response = s3.put_object(
        Body = json.dumps(new_object),
        Bucket ='usu-cs5260-ignite-dist',
        Key = j_data['owner'] + '/' + j_data['widgetId'],
    )
    print('done')
        
    # except ClientError as e:
    #     pass

def create_widget_dynamodb(j_data):
    
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
        
# session = boto3.Session()
# s3_client = session.client('s3')
# b = s3_client.list_buckets()
# for item in b['Buckets']:
#     print(item['Name'])
    
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