import os
import time
from aws_lambda_powertools import Logger
from dynamo_operations import update_finished_processes, create_dynamodb_stream_eventsource


sg_aggregate_table_name = os.environ['SG_AGGREGATE_TABLE_NAME']

"""
# dev only 
sg_aggregate_table_name = 'scatter_gather_aggregate'
"""


logger = Logger()
timestamp = int(time.time())


@logger.inject_lambda_context
def lambda_handler(event, context):
    logger.info({"action":"invoke_lambda", "payload":{"event":event}})
    
    """
    # For dev only
    # Set up Lambda EventSourceMapping, and the aggregator lambda is triggered by modification events ("eventName":["MODIFY"]) from SGProcessesDBTable. 
    SGProcessesDBTable_Stream_arn = 'arn:aws:dynamodb:ca-central-1:006288227511:table/scatter_gather_processes/stream/2023-11-28T17:08:49.729'
    AggregatorFunction_arn = 'arn:aws:lambda:ca-central-1:006288227511:function:eo-sgp-sentinel1-aggregato-EOSGPAggregatorFunction-WOKSHKrDWJq5'
    # Create the DynamoDB stream event source mappin
    response = create_dynamodb_stream_eventsource(lambda_function_arn=AggregatorFunction_arn, dynamodb_stream_arn=SGProcessesDBTable_Stream_arn,batch_size=100, maximum_batching_window_in_seconds=30)
    print(response)
    """
    agg_scatter_gather_id = {}
    for record in event["Records"]:
        new_image = record["dynamodb"]['NewImage']
        status = new_image["status"]['S']
        scatter_gather_id = new_image["scatter_gather_id"]['S']
        
        if status == "finished":
        # Add a entry in agg_scatter_gather_id when one process lambda finish 
            agg_scatter_gather_id[scatter_gather_id] = agg_scatter_gather_id.get(scatter_gather_id, 0) + 1

    logger.info({"action":"agg_scatter_gather_id", "payload":{"agg_scatter_gather_id":agg_scatter_gather_id}})

    # update total_finished values in dynamo for each scatter_gather_id
    for scatter_gather_id, value_to_sum in agg_scatter_gather_id.items():
        update_finished_processes(sg_aggregate_table_name, scatter_gather_id, value_to_sum)

    return { 
        "statusCode": 200, 
        "status": "success"
        }
