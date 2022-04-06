import os
import pandas as pd
import boto3
from boto3.session import Session
import json
from io import StringIO
from modules.macro import macro
from io import BytesIO
from dateutil.parser import parse


access_key_id='AKIAYV2B3AOU3RCKJPVT'
secret_access_key='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'
region_name='us-east-1'
accountId='596601668521'
name=''
s3=[]
bucket='datamasking123'
file=''


def isdate(string, fuzzy=False):
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False

def create_directory(path,access_key_id,secret_access_key,region_name,accountId,s3,bucket,file):
    client=boto3.client('s3',aws_access_key_id=access_key_id,aws_secret_access_key=secret_access_key,region_name=region_name)
    client.put_object(Bucket=bucket, Key=(path+'/'))
    

def insert_to_folder(file_path,s3_path,access_key_id,secret_access_key,region_name,accountId,s3,bucket,file):
    client= boto3.resource('s3',aws_access_key_id=access_key_id,aws_secret_access_key=secret_access_key,region_name=region_name)
    client.meta.client.upload_file(file_path, bucket,s3_path)
    
    
def insert_to_df_folder(df,s3_path,access_key_id,secret_access_key,region_name,accountId,s3,bucket,file):
     csv_buffer = StringIO()
     df.to_csv(csv_buffer, index=False)
     s3_key = s3_path
     session = boto3.session.Session()
     s3_resource = session.resource('s3',aws_access_key_id=access_key_id,aws_secret_access_key=secret_access_key,region_name=region_name)
     s3_resource.Object(bucket, s3_key).put(Body=csv_buffer.getvalue())
    
def insert_dict_folder(dic,s3_path,access_key_id,secret_access_key,region_name,accountId,s3,bucket,file):
    json_object = json.dumps(dic, indent = 4) 
    s3 = boto3.resource('s3',aws_access_key_id=access_key_id,aws_secret_access_key=secret_access_key,region_name=region_name)
    s3object = s3.Object(bucket,f'{s3_path}{file}')
    s3object.put(Body=(bytes(json.dumps(json_object).encode('UTF-8'))))
    
def retrieve_json_data_s3_path(s3_file_path,access_key_id,secret_access_key,region_name,accountId,s3,bucket,file):
    s3 = boto3.resource('s3',aws_access_key_id=access_key_id,aws_secret_access_key=secret_access_key,region_name=region_name)
    content_obj=s3.Object(bucket,s3_file_path)
    file_content=content_obj.get()['Body'].read().decode('utf-8')   
    json_content=json.loads(file_content)
    return json_content
    

def find_files(filename, search_path):
   for root, dir, files in os.walk(search_path):
      if filename in files:
         path=os.path.join(root, filename)
         path=path.replace('\\', '/')
         return path
   return ""