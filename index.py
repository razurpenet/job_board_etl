import requests
import json
import pandas as pd
import configparser
import os
import boto3
from utils.helper import create_bucket


config = configparser.ConfigParser()

config.read('.env')
api_access = config['API_ACCESS']['X-RapidAPI-Key']
api_host = config['API_ACCESS']['X-RapidAPI-Host']

aws_access = config['AWS_ACCESS']['access']
aws_secretkey = config['AWS_ACCESS']['secret_key']
aws_bucketname = config['AWS_ACCESS']['bucketname']
aws_region = config['AWS_ACCESS']['region']

#Data Extraction Layer

#Extract usa data engineer jobs
def extract_de_us_jobs():
    url = "https://jsearch.p.rapidapi.com/search"
    querystring = {"query":"Data Engineer jobs in USA","page":"2","num_pages":"4","date_posted":"today"}
    headers = {
       "X-RapidAPI-Key" : api_access,
        "X-RapidAPI-Host" : api_host
    } 
    response = requests.get(url, headers=headers, params=querystring)

    #Fetch data from api
    data = response.json()
    with open("json_folder/raw_de_us_jobs.json", "w") as file:
        json.dump(data, file)
    
    #df = pd.json_normalize(data['data'])
    # return df

extract_de_us_jobs()

#Extract uk data engineer jobs
def extract_de_uk_jobs():
    url = "https://jsearch.p.rapidapi.com/search"
    querystring = {"query":"Data Engineer jobs in UK","page":"2","num_pages":"4","date_posted":"today"}
    headers = {
       "X-RapidAPI-Key" : api_access,
        "X-RapidAPI-Host" : api_host
    }
    response = requests.get(url, headers=headers, params=querystring)
    #Fetch data from api
    data = response.json()
    with open("json_folder/raw_de_uk_jobs.json", "w") as file:
        json.dump(data, file)
    # df = pd.json_normalize(data['data'])
    # return df

extract_de_uk_jobs()

#Extract canada data engineer jobs
def extract_de_ca_jobs():
    url = "https://jsearch.p.rapidapi.com/search"
    querystring = {"query":"Data Engineer jobs in Canada","page":"2","num_pages":"4","date_posted":"today"}
    headers = {
       "X-RapidAPI-Key" : api_access,
        "X-RapidAPI-Host" : api_host
    } 
    response = requests.get(url, headers=headers, params=querystring)

    #Fetch data from api
    data = response.json()

    with open("json_folder/raw_de_ca_jobs.json", "w") as file:
        json.dump(data, file)
    

    # df = pd.json_normalize(data['data'])
    # print(df)

extract_de_ca_jobs()


#create s3 bucket
#create_bucket()


#Write raw data to s3 bucket
bucketname = aws_bucketname
local_folder = './json_folder'
s3_prefix = 'jobboard101/'

#Create s3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secretkey, region_name=aws_region)

#iterate through local files
for filename in os.listdir(local_folder):
    #print(filename)
    if filename.endswith('.json'):
        local_file_path = os.path.join(local_folder, filename)

        #Read json data from folder
        with open(local_file_path, 'r') as file:
            json_data = file.read()

        #Determine the s3 Key
        s3_key = s3_prefix + filename

        #Upload the json data to s3
        s3.put_object(Bucket=bucketname, Key=s3_key, Body=json_data)

        print(f"Uploaded {filename} to {bucketname}/{s3_key}")

print("All JSON files have been successfully uploaded to s3.")
