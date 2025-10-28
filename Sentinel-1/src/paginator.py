import requests
import json
import logging 
import boto3 
import os
import time
from botocore.exceptions import ClientError
from datetime import datetime


def search_pages_get_json(url: str, collection:str, payload: dict = None, max_retries: int = 5, initial_backoff: float = 1.0):
    """
    A valid list of urls based on stac api link['next'] for the search endpoint
    
    Franklin STAC API generates a next link even when there is no next page
    (https://datacube.services.geo.ca/api/collections/landcover/items)

    This pagenator verifies the validity of the next link and returns a list
    of valid pages. It includes retry logic with exponential backoff to handle
    transient network failures.

    Parameters
    ----------
    url : str
        The stac api endpoint.
    collection : str
        The collection name.
    payload : dict
        The POST payload. The default is None.
    max_retries : int
        Maximum number of retry attempts for failed requests. Default is 5.
    initial_backoff : float
        Initial backoff time in seconds for retry logic. Default is 1.0.

    Returns
    -------
    pages: list
        A list of valid page urls to paginate through.
    
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
        retry_count = 0
        success = False
        r = None
        
        while retry_count < max_retries and not success:
            try: 
                print(f'Fetching page: {next_page} (attempt {retry_count + 1}/{max_retries})')
                r = requests.get(next_page, timeout=60)
                
                if r.status_code == 200:
                    print('Status code is 200')
                    j = r.json()            
                    print('JSON response received')
                    
                    # Test the returns total against total matched
                    returned += j['context']['returned']
                    matched = j['context']['matched']
                    print(f'Progress: {returned}/{matched} items collected')
                    
                    if returned > 0:
                        json_object = {"collection": collection,
                                       "item_api": next_page,
                                       "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                       }
                        pages.append(json_object)
                        print(f'Page appended: {json_object}')
                          
                    if returned < matched:
                        links = j['links']
                        next_page = get_next_page(links)
                        print(f'Next page URL: {next_page}')
                    else:
                        next_page = None
                        print(f'All pages processed. Total items collected: {returned}')
                    
                    success = True
                    
                elif r.status_code >= 500:
                    # Server error - retry with backoff
                    print(f'Server error (status code {r.status_code}), will retry')
                    retry_count += 1
                    if retry_count < max_retries:
                        backoff_time = initial_backoff * (2 ** (retry_count - 1))
                        print(f'Retrying in {backoff_time} seconds...')
                        time.sleep(backoff_time)
                    else:
                        print(f'Max retries reached for {next_page}. Status code: {r.status_code}')
                        raise Exception(f'Failed to fetch page after {max_retries} attempts: HTTP {r.status_code}')
                else:
                    # Client error (4xx) - don't retry, log and fail
                    print(f'Client error: Status code {r.status_code} for {next_page}')
                    raise Exception(f'Client error: HTTP {r.status_code} for {next_page}')
                    
            except requests.exceptions.RequestException as e:
                # Network/connection errors - retry with backoff
                print(f'Request exception: {e}')
                retry_count += 1
                if retry_count < max_retries:
                    backoff_time = initial_backoff * (2 ** (retry_count - 1))
                    print(f'Retrying in {backoff_time} seconds...')
                    time.sleep(backoff_time)
                else:
                    print(f'Max retries reached for {next_page}. Error: {e}')
                    raise Exception(f'Failed to fetch page after {max_retries} attempts: {e}')
            except Exception as e:
                print(f'Unexpected error: {e}')
                print(f'Error occurred while accessing: {next_page}')
                raise
            finally:
                if r is not None:
                    r.close()
                          
    print(f'Pagination complete. Collected {len(pages)} pages with {returned} items total (expected {matched})')
    
    # Validate that we collected all expected items
    if returned != matched:
        print(f'WARNING: Item count mismatch! Collected {returned} items but expected {matched}')
    
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

