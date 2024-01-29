import boto3 
import logging
import os 
import json
from botocore.exceptions import ClientError

def delete_filelist_s3(deleted_filelist, bucket):
    """ Delete the STAC JSON files in deleted_filelist from an s3 bucket
    Return a message to the user: "Deleted xx records from S3 yy bucket"
    :parm deleted_filelist: a list of s3 files to be deleted 
    :parm bucket: s3 bucket to delete from 
    """
    s3 = boto3.resource('s3')
    error_msg = None 
    count = 0
    for filename in deleted_filelist:    
        try: 
            s3object = s3.Object(bucket, filename)
            response = s3object.delete()
            count += 1
            #print("Response: ", response)
            #print(f"Deleted filenames: {filename} from bucket {bucket}")
        except ClientError as e: 
            logging.error(e)
            error_msg += e
    print('Deleted ', count, " records from S3 ", bucket)
        
    return error_msg

def open_file_s3(bucket, filename):
    """Open a S3 file from bucket and filename and return the body as a string
    :param bucket: Bucket name
    :param filename: Specific file name to open
    :return: body of the file as a string
    """
    try: 
        """
        s3 = boto3.client("s3")
        bytes_buffer = io.BytesIO()
        s3.download_fileobj(Key=filename, Bucket=bucket, Fileobj=bytes_buffer)
        file_body = bytes_buffer.getvalue().decode() #python3, default decoding is utf-8
        #print (file_body)
        #print(type(file_body))
        """
        # Second option to load file from S3 buckets 
        s3 = boto3.resource('s3')
        content_object = s3.Object(bucket, filename)
        file_body= content_object.get()['Body'].read().decode('utf-8')
        #json_content = json.loads(file_content)
        
        return str(file_body)
    except ClientError as e:
        logging.error(e)
        return False 
 

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