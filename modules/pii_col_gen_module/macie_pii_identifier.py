import boto3
import time
from modules.helper import insert_dict_folder

def list_finding(access_key_id,secret_access_key,region_name,job_id):
    client = boto3.client('macie2',aws_access_key_id=access_key_id,aws_secret_access_key=secret_access_key,region_name=region_name)
    response = client.list_findings()
    findings=response['findingIds']
    return findings


def create_job(name,s3,access_key_id,secret_access_key,region_name,accountId):
    client = boto3.client('macie2',aws_access_key_id=access_key_id,aws_secret_access_key=secret_access_key,region_name=region_name)
    response = client.create_classification_job(
        clientToken='string',
        jobType='ONE_TIME',
        name=name,
        s3JobDefinition={
            'bucketDefinitions': [
                {
                    'accountId':accountId,
                    'buckets': s3
                },
            ]
     }       
    )
    return response['jobId']


def get_finding(access_key_id,secret_access_key,region_name,job_id):
    client = boto3.client('macie2',aws_access_key_id=access_key_id,aws_secret_access_key=secret_access_key,region_name=region_name)
    findings=list_finding(access_key_id,secret_access_key,region_name,job_id)
    response = client.get_findings(findingIds=findings)
    response=response['findings']
    data=[]
    for i in range(len(response)):
        if response[i]["classificationDetails"]["jobId"] == job_id:
            data.append(response[i])
    
    json_object=json.dumps(data,indent=4, sort_keys=True, default=str)
    insert_dict_folder(json_object,'macieoutput/macie_output.json', access_key_id, secret_access_key, region_name, accountId, s3, bucket, file)


def main(access_key_id,secret_access_key,region_name,accountId,jobName,s3BucketName):
    client = boto3.client('macie2',aws_access_key_id=access_key_id,aws_secret_access_key=secret_access_key,region_name=region_name)
    s3_list=[]
    s3_list.append(s3BucketName)
    job_id=create_job(jobName,s3_list,access_key_id,secret_access_key,region_name,accountId)
    response = client.describe_classification_job(jobId=job_id)
    while (response['jobStatus'] != 'COMPLETE'):
        time.sleep(60)
        response = client.describe_classification_job(jobId=job_id)
        
    get_finding(access_key_id,secret_access_key,region_name,job_id)