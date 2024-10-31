import requests
import os 
import re 
import json
import boto3 
from datetime import datetime
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

logger = Logger()

from paginator import *
from s3_operations import *
from stac_to_geocore_rcm-ard import *



# environment variables for lambda
eo_datacube_api_bucketname =  os.environ['ITEM_LINK_BUCKET_NAME']
geocore_template_bucket_name = os.environ['GEOCORE_TEMPLATE_BUCKET_NAME']
geocore_template_name = os.environ['GEOCORE_TEMPLATE_NAME']
processed_data_bucket_name = os.environ['PROCESSED_DATA_BUCKET_NAME']
api_root = os.environ['API_ROOT']
root_name = os.environ['ROOT_NAME']
source = os.environ['SOURCE']
sourceSystemName = os.environ['SOURCESYSTEMNAME']
collection = os.environ['COLLECTION']


"""
#dev setting  -- comment out for release
eo_datacube_api_bucketname = 'eo-sgp-datacube-item-links' 
geocore_template_bucket_name = 'webpresence-geocore-template-dev' #template json and lastRun txt
geocore_template_name = 'geocore-format-null-template.json'
processed_data_bucket_name = 'eo-sgp-processed-items' #Currently, upload to a thrid bucket so that we do not affect the current process, for production, it should upload to json to geojson bucket 
api_root = 'https://www.eodms-sgdot.nrcan-rncan.gc.ca/stac'
root_name = 'EODMS Datacube API / EODMS Cube de données API' #must provide en and fr 
source='eodms'
sourceSystemName = 'ccmeo-eodms'
collection='sentinel-1'
"""

datetime_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@logger.inject_lambda_context
def lambda_handler(event, context):
  
    #Create the item links json, limit=5000
    pages = search_pages_get_json(url=api_root + '/collections/' +collection+'/items?limit=5000', collection=collection)
    filename = collection+'-item-api.json'
    msg = upload_file_s3(filename, bucket=eo_datacube_api_bucketname, json_data=pages,object_name=None)
    if msg: 
        print(f'Sucessfully uploaded {filename} to bucket: {eo_datacube_api_bucketname}, the length of {filename} is {len(pages)}')   
        
    #Root and Collection Leevl harvesting 
    response_root = requests.get(f'{api_root}/')
    if response_root.status_code == 200:
        try: 
            root_data_json = response_root.json()
            root_id = root_data_json['id']
            if root_id.isspace()==False:
                root_id=root_id.replace(' ', '-')
            root_des = root_data_json['description']
            root_links = root_data_json['links']
            
            # GeoCore properties bounding box is a required for frontend, here we use the first collection
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
        
            # Root level mapping 
            geocore_features_dict = get_geocore_template(geocore_template_bucket_name,geocore_template_name) 
            root_geometry_dict = to_features_geometry(geocore_features_dict, bbox=root_bbox, geometry_type='Polygon')
            
            root_properties_dict = root_to_features_properties(params, geocore_features_dict)
            root_geocore_updated = update_geocore_dict(geocore_features_dict=geocore_features_dict, properties_dict =root_properties_dict ,geometry_dict=root_geometry_dict)
            root_upload = source + '-root-' + root_id + '.geojson'

            msg = upload_file_s3(root_upload, bucket=processed_data_bucket_name, json_data=root_geocore_updated, object_name=None)
            if msg: 
                print(f'Finished mapping root : {root_id}, and uploaded the file to bucket: {processed_data_bucket_name}')
                
            
            # Collection Level mapping 
            print(collection)
            collection_sentinel_list = [entry for entry in collection_data_list if entry.get('id') == collection]
            collection_sentinel_dict =collection_sentinel_list[0]
            coll_extent = collection_sentinel_dict.get('extent')
            coll_bbox = coll_extent.get('spatial', {}).get('bbox', [None])
            
            geocore_features_dict = get_geocore_template(geocore_template_bucket_name,geocore_template_name)
            coll_geometry_dict = to_features_geometry(geocore_features_dict, bbox=coll_bbox, geometry_type='Polygon')
            coll_properties_dict = coll_to_features_properties(coll_dict=collection_sentinel_dict, params=params, geocore_features_dict=geocore_features_dict)
            coll_geocore_updated = update_geocore_dict(geocore_features_dict=geocore_features_dict, properties_dict =coll_properties_dict, geometry_dict=coll_geometry_dict)
            coll_id = collection_sentinel_dict.get('id')
            coll_name = source + '-collection-' + coll_id + '.geojson'
            
            msg = upload_file_s3(coll_name, bucket=processed_data_bucket_name, json_data=coll_geocore_updated, object_name=None)
            if msg: 
                print(f'Mapping Collection: {coll_id}. Finished and Uploaded the collection to bucket: {processed_data_bucket_name}')

        except json.JSONDecodeError:
            # Handle JSON decode error
            print("response_root is not in JSON format. Raw response:", response_root.text)
            
    else: 
        print('Request failed:root_api is {api_root}.  Status code: {response.status_code}. Response: {response.text}')
        



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