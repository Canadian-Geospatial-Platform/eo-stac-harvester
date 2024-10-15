import os
import time
from aws_lambda_powertools import Logger
from dynamo_operations import update_finished_processes, create_dynamodb_stream_eventsource


logger = Logger()
timestamp = int(time.time())

@logger.inject_lambda_context
def lambda_handler(event, context):
    logger.info({"action":"invoke_lambda", "payload":{"event":event}})
    
    """
    # For dev only
    # Set up Lambda EventSourceMapping, and the aggregator lambda is triggered by modification events ("eventName":["MODIFY"]) from SGProcessesDBTable. 
   
    SGAggregatorDBTable_Stream_arn = 'arn:aws:dynamodb:ca-central-1:006288227511:table/scatter_gather_aggregate/stream/2023-11-28T17:08:49.913'
    GatherFunction_arn = 'arn:aws:lambda:ca-central-1:006288227511:function:eo-sgp-sentinel1-gather-EOSGPGatherFunction-WVHrPquFasLS'
    # Create the DynamoDB stream event source mappin
    response = create_dynamodb_stream_eventsource(lambda_function_arn=GatherFunction_arn, dynamodb_stream_arn=SGAggregatorDBTable_Stream_arn,batch_size=100, maximum_batching_window_in_seconds=30)
    print(response)
    """
    
    finished_sg_ids = []
    for record in event["Records"]:
        event_name = record["eventName"]
        new_image = record["dynamodb"]['NewImage']
        finished_processes = new_image["finished_processes"]["N"]
        total_processes = new_image["total_processes"]["N"]
        
        if finished_processes == total_processes:
            finished_sg_ids.append(new_image["scatter_gather_id"]['S'])

    # report ScatterGather finished or trigger your next step
    for scatter_gather_id in finished_sg_ids:
        logger.info({"action":"finished_scatter_gather_id", "payload":{"scatter_gather_id":scatter_gather_id}})
