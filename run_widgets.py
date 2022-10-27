import boto3, json
import sys
import time
from boto3 import client
from botocore.exceptions import ClientError

def check_items(source):
    wait_end = ""
    bucket = 'usu-cs5260-ignite-requests'
    key = 'hello world.txt'
    
    while wait_end != "end":
        wait_end = input()
        conn = client('s3')  
        if len(conn.list_objects(Bucket=bucket)['Contents']) > 0:
            for key in conn.list_objects(Bucket=bucket)['Contents']:
                s3 = boto3.client('s3')
                obj = s3.get_object(Bucket=bucket, Key=key['Key'])
                j_data = json.loads(obj['Body'].read())
                
                
                if source == 's3':
                    create_widget_s3(j_data)
                else:
                    create_widget_dynamodb(j_data)
                
                delete_object(bucket, key['Key'])
        else:
            time.sleep(0.1)

def create_widget_s3(j_data):
    s3 = boto3.client('s3')
    
    
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
        

def create_widget_dynamodb(j_data):
    
    new_object={
            'id': {
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
    for j in j_data['otherAttributes']:
        new_object[j['name']] = {'S' : j['value']}
    
    DDB = boto3.client('dynamodb', region_name='us-east-1')
    
    response = DDB.put_item(
        TableName='widgets',
        Item=new_object,
    )
    
    
        
def delete_object(bucket, object_key):
    s3 = boto3.resource('s3')
    s3.Object(bucket, object_key).delete()


if sys.argv[1] != 's3' and sys.argv[1] != 'dynamodb':
    print("Please choose 's3' or 'dynamodb' to choose a location to store widgets")
else:
    check_items(sys.argv[1])