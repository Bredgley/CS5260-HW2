import boto3
import sys

session = boto3.Session()
s3_client = session.client('s3')
b = s3_client.list_buckets()
for item in b['Buckets']:
    print(item['Name'])
    
try:
    if sys.argv[1] != 's3' and sys.argv[1] != 'dynamodb':
        print("Please choose 's3' or 'dynamodb' to choose a location to store widgets")
    else:
        
except:
    print("Please choose 's3' or 'dynamodb' to choose a location to store widgets")