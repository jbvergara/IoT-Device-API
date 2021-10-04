import json
import logging
import boto3
import os
import arrow
import random


log = logging.getLogger()

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
        
    except KeyError:
        log.error(f'Invalid Input Parameters')
        return {
            "statusCode": 400,
            "body": f"Invalid Input Parameters: {event}",
        }
        
    new_time = convert_time(start_time, 'US/Pacific')
    interval[1] = interval[1] + random.randint(1, 600)
        
    response = {
        "type": data_type,
        "devId": dev_id,
        "startAt": new_time,
        "interval": interval,
        "maxWh": maxWh
    }
        
    return {
        "statusCode": 200,
        "body": json.dumps(response)
    }
    
def convert_time(time, timezone):
    time_format = "YY/MM/DD,HH:mm:ss-S"
    new_format = "YYYY-MM-DDTHH:mm:ss:SSS[Z]"
    arrow_time = arrow.get(time, time_format).to('utc')
    return arrow_time.format(new_format)