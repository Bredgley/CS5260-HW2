import boto3, json
import sys
import time
from boto3 import client
from botocore.exceptions import ClientError

def main():
    if sys.argv[1] != 's3' and sys.argv[1] != 'dynamodb':
        print("Please choose 's3' or 'dynamodb' to choose a location to store widgets")
    else:
        if len(sys.argv) > 2:
            check_items_sqs(sys.argv[1])
        else:
            check_items_s3(sys.argv[1])
            
def check_items_sqs(destination):
    wait_end = ""
    bucket = 'usu-cs5260-ignite-requests'
    f = open("consumer_log.txt", "x")
    f.close()

def check_items_s3(destination):
    wait_end = ""
    bucket = 'usu-cs5260-ignite-requests'
    bucketDest = 'usu-cs5260-ignite-dist'
    f = open("consumer_log.txt", "x")
    
    while wait_end != "end":
        conn = client('s3')  
        if len(conn.list_objects(Bucket=bucket)['Contents']) > 0:
            for key in conn.list_objects(Bucket=bucket)['Contents']:
                s3 = boto3.client('s3')
                obj = s3.get_object(Bucket=bucket, Key=key['Key'])
                j_data = json.loads(obj['Body'].read())
                process_request(key, j_data, bucket, bucketDest, destination)
        else:
            print('.')
            time.sleep(0.1)
            
def process_request(key, j_data, bucket, bucketDest, destination):
    if j_data['type'] == 'create':
        
        f = open("consumer_log.txt", "a")
        f.write("Creating Widget from request:" + j_data['requestId'] + "\n")
        print("Creating Widget from request:" + j_data['requestId'])
        f.close()
        
        if destination == 's3':
            create_widget_s3(j_data)
        else:
            create_widget_dynamodb(j_data)
            
    elif j_data['type'] == 'update':
        
        f = open("consumer_log.txt", "a")
        f.write("Updating Widget from request:" + j_data['requestId'] + "\n")
        print("Updating Widget from request:" + j_data['requestId'])
        f.close()
        
        if destination == 's3':
            s3 = boto3.client('s3')
            newKey = "" + j_data['owner'] + j_data['widgetId']
            obj = s3.get_object(Bucket=bucketDest, Key=newKey)
            j = json.loads(obj['Body'].read())
            j_data = combine_files(j_data, j)
            delete_object(bucketDest, newKey)
            time.sleep(1)
            create_widget_s3(j_data)
            
    else:
        
        f = open("consumer_log.txt", "a")
        f.write("Deleted Widget request:" + j_data['requestId'] + "\n")
        print("Deleted Widget request:" + j_data['requestId'])
        f.close()
        
        if destination == 's3':
            newKey = "" + j_data['owner'] + j_data['widgetId']
            delete_object(bucketDest, newKey)
        
    delete_object(bucket, key['Key'])
    f = open("consumer_log.txt", "a")
    f.write("Deleted Widget request:" + j_data['requestId'] + "\n")
    print("Deleted Widget request:" + j_data['requestId'])
    f.write("=======" + "\n")
    f.close()

def combine_files(jsonNew, jsonOld)
    jsonCombine = jsonOld
    for key in jsonCombine:
        jsonCombine[key] = jsonNew[key]
    return jsonCombine

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


main()
        