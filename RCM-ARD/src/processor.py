import requests
import os 
import re 
import json
import boto3 
from datetime import datetime
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

logger = Logger()

from s3_operations import *
from stac_to_geocore_rcm_ard import *
from dynamo_operations import update_item_finished


# environment variables for lambda
geocore_template_bucket_name = os.environ['GEOCORE_TEMPLATE_BUCKET_NAME']
geocore_template_name = os.environ['GEOCORE_TEMPLATE_NAME']
processed_data_bucket_name = os.environ['PROCESSED_DATA_BUCKET_NAME']
api_root = os.environ['API_ROOT']
root_name = os.environ['ROOT_NAME']
source = os.environ['SOURCE']
sourceSystemName = os.environ['SOURCESYSTEMNAME']
collection = os.environ['COLLECTION']
sg_processes_table_name  = os.getenv('SG_PROCESSES_TABLE_NAME') 


"""
#dev setting  -- comment out for release
geocore_template_bucket_name = 'webpresence-geocore-template-dev' #template json and lastRun txt
geocore_template_name = 'geocore-format-null-template.json'
#geocore_to_parquet_bucket_name =
processed_data_bucket_name = "eo-sgp-processed-items" #Currently, upload to a thrid bucket so that we do not affect the current process, for production, it should upload to json to geojson bucket 
api_root = 'https://www.eodms-sgdot.nrcan-rncan.gc.ca/stac'
root_name = "EODMS Datacube API / EODMS Cube de données API" #must provide en and fr 
source='eodms'
sourceSystemName = 'ccmeo-eodms'
collection='sentinel-1'
sg_processes_table_name  = 'scatter_gather_processes'
"""

datetime_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@logger.inject_lambda_context
def lambda_handler(event, context):
    """STAC harvesting and mapping workflow 
        1. Before harvesting the stac records, we delete the previous harvested stac records logged in lastRun.txt.
        2. Create an empty lastRun.txt to log the current harvest
        3. Loop through items within the collection, harvest the item json bady and map item to GeoCore. 
        4. Update lastRun.txt 
    """
    #sample event in events/processor.json
    logger.info({"action":"invoke_lambda", "payload":{"event":event}})
    json_body = json.loads(event["Records"][0]["body"])
    item_event = json_body.get("item-api")
    index_event = json_body.get("index")
    
    lastrun = f'lastRun_eodms_{collection}_{index_event}.txt'
    
    error_msg = ''
    #Change directory to /tmp folder, required if new files are created in this lambda 
    os.chdir('/tmp')    
    if not os.path.exists(os.path.join('mydir')):
        os.makedirs('mydir')

    # Start the harvest and translation process if connection is okay 
    response_root = requests.get(f'{api_root}')
    if response_root.status_code == 200:
        e = delete_stac_s3(bucket_geojson=processed_data_bucket_name, bucket_template=geocore_template_bucket_name, filename=lastrun)
        if e != None: 
            error_msg += e
        # Create a new log file for each sucessfull item harvest  
        print(f'Creating a new {lastrun}')
        with open(f'/tmp/{lastrun}', 'w') as f:
            # Catalog Level  
            try: 
                root_data_json = response_root.json()
                root_id = root_data_json['id']
                if root_id.isspace()==False:
                    root_id=root_id.replace(' ', '-')
                root_des = root_data_json['description']
                root_links = root_data_json['links']
    
               # GeoCore properties bounding box is a required for frontend, here we use the first collection
               #?TBD using first collection bounding box could cause potential issues when collections have different extent, a solution is required. 
                response_collection = requests.get(f'{api_root}/collections/')
                collection_data_list = response_collection.json()['collections']
                root_bbox = collection_data_list[1]['extent']['spatial']['bbox']
                
                # Perpare for parametes required for the function 
                params = {
                        'root_name': root_name, 
                        'root_links': root_links, 
                        'root_id': root_id, 
                        'root_des': root_des, 
                        'root_bbox': root_bbox,
                        'source': source,  
                        'status':status,
                        'maintenance':maintenance, 
                        'useLimits_en': useLimits_en,
                        'useLimits_fr': useLimits_fr,
                        'spatialRepresentation': spatialRepresentation,
                        'contact': contact,
                        'type_data': type_data,
                        'topicCategory': topicCategory,
                        'sourceSystemName':sourceSystemName,
                        'eoCollection':collection,
                        "coll_description_en": coll_description_en, 
                        "coll_description_fr": coll_description_fr,
                        "coll_keywords_fr": coll_keywords_fr 
                        }
                        
                item_count = 0 
                # Get the collection level keywords, description, and titles   
                coll_id_dict = create_coll_dict(api_root, collection, coll_description_en,coll_description_fr,coll_keywords_fr)
                
                r = requests.get(item_event)
                if r.status_code == 200:
                    try:
                        j = r.json()
                        items_list = j['features']
                        for item in items_list: 
                            item_id, item_bbox, item_links, item_assets, item_properties,coll_id = get_item_fields(item)
                            print(f'Starting maping item {item_count}: {item_id}')
                            geocore_features_dict = get_geocore_template(geocore_template_bucket_name,geocore_template_name)
                            item_geometry_dict = to_features_geometry(geocore_features_dict=geocore_features_dict, bbox=item_bbox, geometry_type='Polygon')
                                
                            item_properties_dict = item_to_features_properties(params=params, geocore_features_dict=geocore_features_dict, item_dict=item, coll_id_dict=coll_id_dict)
                            item_geocore_updated = update_geocore_dict(geocore_features_dict=geocore_features_dict, properties_dict =item_properties_dict ,geometry_dict=item_geometry_dict)
                            item_name = source + '-' + coll_id + '-' + item_id + '.geojson'
                            

                            msg = upload_file_s3(item_name, bucket=processed_data_bucket_name, json_data=item_geocore_updated, object_name=None)
                            
                            if msg == True: 
                                item_count += 1 
                                print(f'Finished and uploaded the item to bucket: {processed_data_bucket_name}')  
                                f.write(f"{item_name}\n")
                            """
                            # add the logging information with logger 
                            logger.info({"action":"harvest and translate item", "payload":{"item_event":item_event, "db":ATHENA_RAW_DATABASE_NAME}})
                            logger.info({"action":"Upload item to S3", "payload":{"s3":processed_data_bucket_name}})
                            """
                    except json.JSONDecodeError:
                        print("Response is not in JSON format. Raw response:", r.text)
                    
                else: 
                    print('Request failed:r is {item_event}.  Status code: {r.status_code}. Response: {r.text}')
            except json.JSONDecodeError:
                # Handle JSON decode error
                print("response_root is not in JSON format. Raw response:", response_root.text)
                    
        f.close()
        print(f'This is lastrun: {lastrun}')
        msg = upload_file_s3(filename=lastrun, bucket=geocore_template_bucket_name, json_data = None, object_name=None)
        if msg == True: 
            print(f'Finished mapping the EODMS Sentinal datacube and uploaded the lastRun.txt to bucket: {geocore_template_bucket_name}')   
    else:
        print('Request failed:root_api is {api_root}.  Status code: {response.status_code}. Response: {response.text}')
        
    
    # Update the status item in Dynamo, when a Processor Function finishes, it updates the item status to “finished” in the DynamoDB SGProcessesDBTable
    item_state = json_body.get("item_state") #item-state included one item from the scatter_gather_processes table 
    update_item_finished(sg_processes_table_name, item_state)
    logger.info({"action":"update_item", "payload":{"item_state":item_state}})

    return {
        "statusCode": 200 if not error_msg else 400,
        "status": "success" if not error_msg else "error"
        }
    

# requires open_file_s3()
def get_geocore_template(geocore_template_bucket_name,geocore_template_name):
    """Getting GeoCore null template from S3 bucket  
    Parameters:
    - geocore_template_bucket_name: S3 bucket name that stores the GeoCore template.
    - geocore_template_name: Name of the GeoCore template file.
    
    Returns:
    - A dictionary containing the GeoCore feature, or None if an error occurs. 
    """  
    try: 
        template= open_file_s3(geocore_template_bucket_name, geocore_template_name)
        if not template: 
            logging.error("Template not found.")
            return None           
        geocore_dict = json.loads(template)
        return geocore_dict['features'][0] 
    except ClientError as e:
        logging.error(f"An error occurred while accessing S3: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"An error occurred while decoding JSON: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None