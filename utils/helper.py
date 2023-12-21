import boto3
import configparser
import redshift_connector as rdc

config = configparser.ConfigParser()

config.read('.env')

aws_access = config['AWS_ACCESS']['access']
aws_secretkey = config['AWS_ACCESS']['secret_key']
aws_bucketname = config['AWS_ACCESS']['bucketname']
aws_region = config['AWS_ACCESS']['region']


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


def connect_to_dwh(conn_details):
    return rdc.connect(**conn_details)