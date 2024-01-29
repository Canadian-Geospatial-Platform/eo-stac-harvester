import boto3
from datetime import datetime
from aws_lambda_powertools import Logger

logger = Logger()
dynamodb = boto3.resource('dynamodb')
status_finished = "finished"
region = 'ca-central-1'

def create_item(table_name, item):
    logger.info({"action":"create_dynamo_item", "payload":{"table_name":table_name, "item":item}})
    table = dynamodb.Table(table_name)
    datetime_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    item["created_at"] = datetime_now
    item["updated_at"] = datetime_now
    try:
        table.put_item(Item=item)
        logger.info({"action":"created_dynamo_item", "payload":{"item":item}})
    except Exception as exception:
        logger.error({"action":"created_dynamo_item", "payload":{"exception":exception, "table_name":table_name, "item":item}})

def update_item_finished(table_name, item):
    logger.info({"action":"update_dynamo_item", "payload":{"table_name":table_name, "item":item}})
    table = dynamodb.Table(table_name)
    datetime_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        table.update_item(
            Key={
                'scatter_gather_id': item["scatter_gather_id"],
                'process_id': item["process_id"]
            },
            UpdateExpression='SET #status = :status, #updated_at = :updated_at',
            ExpressionAttributeNames={
                '#status': 'status',
                '#updated_at': 'updated_at'
            },
            ExpressionAttributeValues={
                ':status': status_finished,
                ':updated_at': datetime_now
            }
        )
        logger.info({"action":"updated_dynamo_item", "payload":{"item":item}})
    except Exception as exception:
        logger.error({"action":"updated_dynamo_item", "payload":{"exception":exception, "table_name":table_name, "item":item}})

def update_finished_processes(table_name, scatter_gather_id, value_to_sum):
    logger.info({"action":"update_finished_processes", "payload":{"table_name":table_name, "scatter_gather_id":scatter_gather_id, "value":value_to_sum}})
    table = dynamodb.Table(table_name)
    datetime_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        table.update_item(
            Key={
                'scatter_gather_id': scatter_gather_id
            },
            UpdateExpression='SET finished_processes = finished_processes + :val, #updated_at = :updated_at',
            ExpressionAttributeValues={
                ':val': value_to_sum,
                ':updated_at': datetime_now
            },
            ExpressionAttributeNames={
                '#updated_at': 'updated_at'
            },
            ReturnValues='ALL_NEW'
        )
        logger.info({"action":"updated_finished_processes", "payload":{"scatter_gather_id":scatter_gather_id, "value":value_to_sum}})
    except Exception as exception:
        logger.error({"action":"updated_finished_processes", "payload":{"exception":exception, "table_name":table_name, "scatter_gather_id":scatter_gather_id, "value":value_to_sum}})

def create_table_process(table_name='scatter_gather_processes', dynamodb=None):
    """     
    """
    dynamodb = boto3.client('dynamodb',region_name=region)
    table_schema = [
            {
                'AttributeName': 'scatter_gather_id',
                'KeyType': 'HASH'  # Partition key
            }, 
            {
                'AttributeName': 'process_id',
                'KeyType': 'RANGE'
                
            }
        ]
    attribute_definitions = [
            {
                'AttributeName': 'scatter_gather_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'process_id',
                'AttributeType': 'S'
                
            }
        ]
    try: 
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=table_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
            },
            StreamSpecification={
            'StreamEnabled': True,
            'StreamViewType': 'NEW_IMAGE'
            },
           # BillingMode='PAY_PER_REQUEST'
        )
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        print(f"Table {table_name} created successfully!")
        return table
    except Exception as exception:
        print(exception) # we can log the error 

def create_table_aggregate(table_name='scatter_gather_aggregate', dynamodb=None):
    """     
    """
    dynamodb = boto3.client('dynamodb',region_name=region)
    table_schema = [
            {
                'AttributeName': 'scatter_gather_id',
                'KeyType': 'HASH'  # Partition key
            }
        ]
    attribute_definitions = [
            {
                'AttributeName': 'scatter_gather_id',
                'AttributeType': 'S'
            }
        ]
    try: 
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=table_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
            },
            StreamSpecification={
            'StreamEnabled': True,
            'StreamViewType': 'NEW_IMAGE'
            },
            #BillingMode='PAY_PER_REQUEST'
        )
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        print(f"Table {table_name} created successfully!")
        return table
    except Exception as exception:
        print(exception) # we can log the error 
   
   
def create_SQS(Name='processor_q', Timeout='900'): 
    # Create an SQS client
    sqs = boto3.client('sqs')
    
    try:
        # Create an SQS queue
        response = sqs.create_queue(
            QueueName=Name,
            Attributes={
                'VisibilityTimeout': Timeout  # The visibility timeout in seconds, set it as the same for the lambda timeout 
            }
        )
        # Print out the response
        print(response)
    except Exception as e:
        # Handle AWS service errors (e.g., queue already exists, permissions issue)
        print(f"Error: {e}")
        

def create_dynamodb_stream_eventsource(lambda_function_arn, dynamodb_stream_arn, batch_size=100, maximum_batching_window_in_seconds=30):
    """
    Create an event source mapping for a DynamoDB stream to trigger a Lambda function.

    :param lambda_function_arn: ARN of the Lambda function
    :param dynamodb_stream_arn: ARN of the DynamoDB stream
    :param batch_size: Number of records to read from the stream in each batch
    :param maximum_batching_window_in_seconds: Maximum amount of time to gather records before invoking the function
    """
    lambda_client = boto3.client('lambda')

    try:
        response = lambda_client.create_event_source_mapping(
            EventSourceArn=dynamodb_stream_arn,
            FunctionName=lambda_function_arn,
            StartingPosition='LATEST',
            BatchSize=batch_size,
            MaximumBatchingWindowInSeconds=maximum_batching_window_in_seconds,
            Enabled=True,
            FilterCriteria={
                'Filters': [
                    {
                        'Pattern': '{"eventName":["MODIFY"]}'
                    }
                ]
            }
        )
        return response
    except Exception as e:
        print(f"Error creating DynamoDB stream event source mapping: {str(e)}")
        return None
