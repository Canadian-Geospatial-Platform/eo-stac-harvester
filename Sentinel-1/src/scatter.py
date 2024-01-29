import os
import boto3
import json
import time
import awswrangler as wr
from aws_lambda_powertools import Logger
from dynamo_operations import *
from s3_operations import * 

logger = Logger()

item_link_bucket_name = os.environ['ITEM_LINK_BUCKET_NAME']
filename = os.environ['JSON_FILENAME']
sg_processes_table_name = os.environ['SG_PROCESSES_TABLE_NAME']
sg_aggregate_table_name= os.environ['SG_AGGREGATE_TABLE_NAME']
queue_url = os.environ['QUEUE_URL']

"""
#For dev only
item_link_bucket_name = 'eo-sgp-datacube-item-links'
filename = 'sentinel-1-item-api.json'
sg_processes_table_name = 'scatter_gather_processes'
sg_aggregate_table_name = 'scatter_gather_aggregate'
queue_url = 'https://sqs.ca-central-1.amazonaws.com/006288227511/processor_q_v2'
"""


@logger.inject_lambda_context

def lambda_handler(event, context):
    """
    Jobs are scatter by item-api in the S3 bucket.
    For each item-api, SQS sends the message queue, and we want to Trigger processor_lambda by item-api
    
    """
    logger.info({"action":"invoke_lambda", "payload":{"event":event}})
    timestamp = int(time.time())
    
    # Read the eo-item-links json from the S3 bucket 
    item_body = open_file_s3(item_link_bucket_name, filename)
    item_json = json.loads(item_body)

    
    """
    # For dev only  
    create_table_process(table_name='scatter_gather_processes', dynamodb=None)
    create_table_aggregate(table_name='scatter_gather_aggregate', dynamodb=None)
    create_SQS(Name='processor_q_v2', Timeout='900')
    """
    
    # write in SG_AGGREGATE_TABLE_NAME DynamoDB table how many api links should be executed
    item = {
        "scatter_gather_id" : str(timestamp),
        "total_processes": len(item_json),
        "finished_processes": 0
        }
    create_item(sg_aggregate_table_name, item)
    
    # For each item-api link send the SQS message queue
    # We want to Trigger processor_lambda by api links
    for index, item_link in enumerate(item_json):
        #print(f'the index is {index}, and the api is {item_link["item_api"]}' )
        
        #store dynamo data by processes triggered
        item_state = {
            "scatter_gather_id" : str(timestamp),
            "process_id" : "{}_{}".format(index, item_link["item_api"]),
            "status": "started"
        }
        create_item(sg_processes_table_name, item_state)
        # create the message queue for the processor lambda payload json 
        formatted_message = '{{"item-api":"{}", "index":{}, "item_state":{}}}'.format(
            item_link["item_api"], index, json.dumps(item_state))
        logger.info({"action":"message_queue", "payload":{"message":formatted_message}})
        send_message_queue(formatted_message)
        print(f'This is formatted_message\n{formatted_message}\n')
 
    return { 
        "statusCode": 200, 
        "status": "success",
        "data": {"states": len(item_json)}
        }
        
def send_message_queue(message):
    sqs_client =boto3.client("sqs")
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=str(message)
    )
    logger.info({"action":"send_message_queue", "payload":{"message":message,"response":response}})
