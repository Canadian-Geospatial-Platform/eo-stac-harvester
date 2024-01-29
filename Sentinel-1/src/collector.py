import requests
import json
import logging 
import boto3 
import os
from botocore.exceptions import ClientError
from datetime import datetime
from patinator import *

"""
eo_datacube_api_bucketname =  os.environ['ITEM_LINK_BUCKET_NAME']
api_root = os.environ['API_ROOT']
collection = os.environ['COLLECTION']
"""

#dev setting  -- comment out for release
eo_datacube_api_bucketname = 'eo-sgp-datacube-item-links'
api_root = 'https://www.eodms-sgdot.nrcan-rncan.gc.ca/stac'
collection = 'sentinel-1'

def lambda_handler(event, context):
  
    #Change directory to /tmp folder, required if new files are created for lambda 
    os.chdir('/tmp')    
    if not os.path.exists(os.path.join('mydir')):
        os.makedirs('mydir')
        
    pages = search_pages_get_json(url=api_root + '/collections/' +collection+'/items?limit=500', collection=collection)
    filename = collection+'-item-api.json'
    print(f'Tyoe of pages is {type(pages)}')
    
    msg = upload_file_s3(filename, bucket=eo_datacube_api_bucketname, json_data=pages, object_name=None)
    if msg == True: 
        print(f'Sucessfully uploaded {filename} to bucket: {eo_datacube_api_bucketname}, the length of {filename} is {len(pages)}')   
    
    """#Debug only 
    pages = [
        {
        "collection": "sentinel-1",
        "item_api": "https://www.eodms-sgdot.nrcan-rncan.gc.ca/stac/collections/sentinel-1/items?limit=500",
        "created_at": "2023-11-19"
    },
    {
        "collection": "sentinel-1",
        "item_api": "http://www.eodms-sgdot.nrcan-rncan.gc.ca/stac/collections/sentinel-1/items?limit=500&token=next:b9ba1c7d-0029-487b-9890-fdfafb190953",
        "created_at": "2023-11-19"
    }
        ]
    
    # Count the number of the items and pagenated links 
    item_count = 0 
    for page in pages:
        item_api = page.get('item_api')
        r = requests.get(item_api)
        j = r.json()
        items_list = j['features']
        item_count += len(items_list)
    print(f'The overall item count is {item_count}')
"""