from dotenv import load_dotenv
import os
import boto3
load_dotenv()
S3_ACCESS_KEY = os.getenv("AWS_S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("AWS_S3_SECRET_KEY")
s3 = boto3.client("s3", aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)

bucket = 'spotifymodel'
s3_dir = 'review-data/reviewdata.txt'


def upload_s3(input_data:str):
    try:
        obj = s3.get_object(Bucket=bucket, Key=s3_dir)
        data = obj['Body'].read().decode()
        new_data = data + input_data
        s3.put_object(Bucket=bucket, Key=s3_dir, Body=new_data.encode())
        return True    
    except:
        return False
    