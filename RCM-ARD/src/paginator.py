import requests
import json
import logging 
import boto3 
import os
from botocore.exceptions import ClientError
from datetime import datetime


def search_pages_get_json(url: str, collection:str, payload: dict = None):
    """
    Returns a JSON list of urls from the stac api link['next'] and validates it.
    It is used as a pointer to the next X records (normally 5000) and is used 
    to paginate the entire STAC items endpoint
    
    Note that the Franklin STAC API generates a next link even when there is no [next] page
    (https://datacube.services.geo.ca/api/collections/landcover/items)

    This paginator validates the [next] link and returns a list
    of valid pages.
    
    This function is called in collector.py

    Parameters
    ----------
    url : str
        The stac api endpoint.

    Returns
    -------
    pages: list
        A list of valid page urls to paginate through.
    payload: dict
        The POST payload.
        The default is None.
    
    Example
    -------
    url = 'datacube.services.geo.ca/collections/msi/items'
    pages = stac_api_paginate(url)
    for page in pages:
        r = requests.get(page)
        ...

    """
    # Get a list of collections from /collections endpoint
    pages = []
    next_page = url
    returned = 0
    matched = 0
   
    while next_page:
        try: 
            #print(f'Trying to get page: {next_page}')
            r = requests.get(next_page)
            #print(f'request response is {r}')
            
            if r.status_code == 200:
                #print('Status code is 200')
                j = r.json()            
                #print('JSON response received')
                
                # Test the returns total against total matched
                returned += j['context']['returned']
                matched = j['context']['matched']
                if returned > 0:
                    json_object = {"collection": collection,
                                   "item_api": next_page,
                                   #"created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                   #"created_at": datetime.datetime.utcnow().now().isoformat()[:-7] + 'Z' #UTC
                                   "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                   
                                   }
                    pages.append(json_object)
                    #print(f'Page appended: {json_object}')
                      
                if returned < matched:
                    links = j['links']
                    next_page = get_next_page(links)
                    #print(f'Next page URL: {next_page}')
                    
                else:
                    next_page = None
                    #print('No more pages to process')
            else:
                #print(f'Status code is not 200: {r.status_code}')
                next_page = None
        except Exception as e:
            print(f'Error: {e}')
            print(f'Connectivity issue: error trying to access the next page api: {next_page}')
                          
    r.close()
    return pages

def get_next_page(links:list):
    """Returns the next page link or None from STAC API Search links list"""
    next_page = None
    for link in links:
        if link['rel'] == 'next':
            next_page = link['href']
    return next_page

# Upload a a text or json file to S3 
def upload_file_s3(filename, bucket, json_data, object_name=None):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param json_data: json_data to be updated, can be none 
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(filename)
    # boto3.client vs boto3.resources:https://www.learnaws.org/2021/02/24/boto3-resource-client/ 
    s3_client = boto3.client('s3')  
    if json_data: 
        try:
            response = s3_client.put_object(Body=(bytes(json.dumps(json_data, indent=4, ensure_ascii=False).encode('utf-8'))), 
                                            Bucket=bucket,
                                            Key = filename)
                                            
        except ClientError as e:
            logging.error(e)
            return False    
    else:     
        try: 
            response = s3_client.upload_file(filename, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False 
    return True 

