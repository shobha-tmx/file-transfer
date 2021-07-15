import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')

file_patterns_table = dynamodb.Table('FILE_PATTERNS')

files_to_be_processed_table = dynamodb.Table('FILES_TO_BE_PROCESSED')

#response = file_patterns_table.scan(FilterExpression=Attr('input_file_pattern').eq('BTALogger.*.csv'))
response = file_patterns_table.scan()
data = response['Items']

while 'LastEvaluatedKey' in response:
    response = file_patterns_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    data.extend(response['Items'])

file_patterns = []

for obj in data:
    file_patterns.append(obj['input_file_pattern'])

def lambda_handler(event, context):
    print(file_patterns)
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        for fp in file_patterns:
            if fp.split('.')[0] in key and key.endswith(fp.split('.')[-1]):
                # Insert into register table
                resp = files_to_be_processed_table.put_item(
                    Item = {
                      "event_id": "N/A",
                      "source": "s3://maps3-tlgcommon",
                      "file" : key,
                      "state": "pending",
                      "retry": "true"
                    }
                )
                print(key, " registed successfully.")
            else:
                print("Nothing to be printed")