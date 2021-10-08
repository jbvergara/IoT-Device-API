import json
import logging
import boto3
import os
import arrow
import random
import requests
import base64
from botocore.exceptions import ClientError
from datetime import date

log = logging.getLogger()
table_name = os.environ.get('dynamodbName')
bucket_name = os.environ.get('bucketName')
slack_url = os.environ.get('slackUrl')
region_name = os.environ.get('AWS_REGION')
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
today = date.today()


def lambda_handler(event, context):
    #Obtain and Check Input Parameters
    try:
        #Obtain Input Parameters
        payload = json.loads(event['body'])
        data_type = payload['type']
        dev_id = payload['devId']
        start_time = payload['startAt']
        interval = payload['interval']
        maxWh = payload['maxWh']
        if(check_opt_out(dev_id, start_time)):
            return {
            "statusCode": 200,
            "body": "Device is currently opted-out"
        }
        new_time = convert_time(start_time, dev_id)
        interval[1] = interval[1] + random.randint(1, 600)
        
    except:
        log.error(f'Invalid Input Parameters')
        return {
            "statusCode": 400,
            "body": f"Invalid Input Parameters: {event}",
        }
    
    else:
        apiResponse = {
            "type": data_type,
            "devId": dev_id,
            "startAt": new_time,
            "interval": interval,
            "maxWh": maxWh
        }
        
        received_token = json.loads(get_secret())
        token = received_token["token"]
        
        s3Response = {
            "Header": {
                "Authorization": token
            },
            "Body": apiResponse
        }
    
        
        filename = f'/tmp/{dev_id}-{data_type}.json'
        path = today.strftime("%Y/%m/%d")
        with open(filename, 'w') as file:
            file.write(json.dumps(s3Response))
        with open(filename, 'rb') as file:
            s3.upload_fileobj(file, bucket_name, f'{path}/{dev_id} - {random.randint(1, 1000)}.json')
        
        slack_data = {
            "text": f"{dev_id} Completed"
        }
        
        headers = {
            "Content-Type":"application/json"
        }
        
        slack_response = requests.post(slack_url, data=json.dumps(slack_data), headers=headers)
        print(slack_response, slack_data)
            
        return {
            "statusCode": 200,
            "body": json.dumps(apiResponse)
        }

def convert_time(time, device_id):
    timezone = get_timezone(device_id)
    time_format = "YY/MM/DD,HH:mm:ss-S"
    new_format = "YYYY-MM-DDTHH:mm:ss:SSS[Z]"
    arrow_time = arrow.get(time, time_format)
    arrow_time = arrow_time.replace(tzinfo=timezone)
    arrow_time = arrow_time.to('utc')
    return arrow_time.format(new_format)
    
def check_opt_out(uid, reference_time):
    table = dynamodb.Table(table_name)
    response = table.get_item(Key={'device_id': uid})
    start_opt_out = response['Item']['local_start_opt_out']
    end_opt_out = response['Item']['local_end_opt_out']
    start = int(start_opt_out[0:2]) + (int(start_opt_out[3:])*0.01)
    end = int(end_opt_out[0:2]) + (int(end_opt_out[3:])*0.01)
    reference = int(reference_time[9:11]) + (int(reference_time[12:13])*0.01)
    if (start <= reference <= end):
        return True
    else:
        return False
        
def get_timezone(uid):
    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(Key={'device_id': uid})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']['timezone']
        
def get_secret():

    secret_name = "arn:aws:secretsmanager:us-west-2:261943945236:secret:dev/token2-q4KEp9"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            
    # Your code goes here. 
        return secret