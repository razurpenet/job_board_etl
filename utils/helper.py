import boto3
import configparser
import redshift_connector as rdc

config = configparser.ConfigParser()

config.read('.env')

aws_access = config['AWS_ACCESS']['access']
aws_secretkey = config['AWS_ACCESS']['secret_key']
aws_bucketname = config['AWS_ACCESS']['bucketname']
aws_transformedbucket = config['AWS_ACCESS']['transformed_bucketname']
aws_region = config['AWS_ACCESS']['region']

#Create s3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secretkey, region_name=aws_region)

#Check if bucket exists 
def does_bucket_exists(aws_bucketname):
    try:
        s3.head_bucket(Bucket=aws_bucketname)
        return True
    except Exception as e:
        return False
    
#Create Raw jobs s3 bucket function
def create_bucket():    
    client = boto3.client(
        's3',
        aws_access_key_id=aws_access,
        aws_secret_access_key=aws_secretkey,

    )
    
    client.create_bucket(
        Bucket=aws_bucketname,
        CreateBucketConfiguration={
            'LocationConstraint': aws_region
        }
    )
#Create Transformed-jobs bucket function
def create_transformed_jobs_bucket():    
    client = boto3.client(
        's3',
        aws_access_key_id=aws_access,
        aws_secret_access_key=aws_secretkey,

    )

    client.create_bucket(
        Bucket=aws_transformedbucket,
        CreateBucketConfiguration={
            'LocationConstraint': aws_region
        }
    )


def connect_to_dwh(conn_details):
    return rdc.connect(**conn_details)