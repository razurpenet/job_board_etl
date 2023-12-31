import requests
import json
import pandas as pd
import configparser
import os
import boto3
from datetime import datetime
from io import StringIO
from utils.helper import create_bucket


config = configparser.ConfigParser()

config.read('.env')
api_access = config['API_ACCESS']['X-RapidAPI-Key']
api_host = config['API_ACCESS']['X-RapidAPI-Host']

aws_access = config['AWS_ACCESS']['access']
aws_secretkey = config['AWS_ACCESS']['secret_key']
aws_bucketname = config['AWS_ACCESS']['bucketname']
aws_transformedbucket = config['AWS_ACCESS']['transformed_bucketname']
aws_region = config['AWS_ACCESS']['region']

#Data Extraction Layer

#Extract usa data engineer jobs
# def extract_de_us_jobs():
#     url = "https://jsearch.p.rapidapi.com/search"
#     querystring = {"query":"Data Engineer jobs in USA","page":"2","num_pages":"4","date_posted":"today"}
#     headers = {
#        "X-RapidAPI-Key" : api_access,
#         "X-RapidAPI-Host" : api_host
#     } 
#     response = requests.get(url, headers=headers, params=querystring)

#     #Fetch data from api
#     data = response.json()
#     with open("json_folder/raw_de_us_jobs.json", "w") as file:
#         json.dump(data, file)
    
#     #df = pd.json_normalize(data['data'])
#     # return df

# extract_de_us_jobs()

# #Extract uk data engineer jobs
# def extract_de_uk_jobs():
#     url = "https://jsearch.p.rapidapi.com/search"
#     querystring = {"query":"Data Engineer jobs in UK","page":"2","num_pages":"4","date_posted":"today"}
#     headers = {
#        "X-RapidAPI-Key" : api_access,
#         "X-RapidAPI-Host" : api_host
#     }
#     response = requests.get(url, headers=headers, params=querystring)
#     #Fetch data from api
#     data = response.json()
#     with open("json_folder/raw_de_uk_jobs.json", "w") as file:
#         json.dump(data, file)
#     # df = pd.json_normalize(data['data'])
#     # return df

# extract_de_uk_jobs()

# #Extract canada data engineer jobs
# def extract_de_ca_jobs():
#     url = "https://jsearch.p.rapidapi.com/search"
#     querystring = {"query":"Data Engineer jobs in Canada","page":"2","num_pages":"4","date_posted":"today"}
#     headers = {
#        "X-RapidAPI-Key" : api_access,
#         "X-RapidAPI-Host" : api_host
#     } 
#     response = requests.get(url, headers=headers, params=querystring)

#     #Fetch data from api
#     data = response.json()

#     with open("json_folder/raw_de_ca_jobs.json", "w") as file:
#         json.dump(data, file)
    

#     # df = pd.json_normalize(data['data'])
#     # print(df)

# extract_de_ca_jobs()


# #create s3 bucket
# create_bucket()


#Write raw data to s3 bucket
bucketname = aws_bucketname
# local_folder = './json_folder'
# s3_prefix = 'jobboard101/'

#Create s3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secretkey, region_name=aws_region)

# #iterate through local files
# for filename in os.listdir(local_folder):
#     #print(filename)
#     if filename.endswith('.json'):
#         local_file_path = os.path.join(local_folder, filename)

#         #Read json data from folder
#         with open(local_file_path, 'r') as file:
#             json_data = file.read()

#         #Determine the s3 Key
#         s3_key = s3_prefix + filename

#         #Upload the json data to s3
#         s3.put_object(Bucket=bucketname, Key=s3_key, Body=json_data)

#         print(f"Uploaded {filename} to {bucketname}/{s3_key}")

# print("All JSON files have been successfully uploaded to s3.")


#Read from raw data in s3 bucket -jobboard101

def read_from_raw_s3():

    s3prefix = 'jobboard101/'
    #List objects in a s3 bucket
    objects = s3.list_objects(Bucket=bucketname, Prefix=s3prefix)
    file = objects.get('Contents')[2]
    key = file.get('Key') # Get file path or key

    response = s3.get_object(Bucket=bucketname, Key=key)
    json_data = response['Body'].read().decode('utf-8')
    df = pd.read_json(StringIO(json_data), typ='series')
    job_details = df.get('data')

    #write to Dataframe

    df = pd.DataFrame(job_details)
    #df.to_csv(file, index=False)

    #Select required column from dataframe
    transformed_column = df[['employer_website', 'job_id', 'job_title', 'job_apply_link', 
                            'job_description', 'job_city', 'job_country', 
                            'job_posted_at_timestamp', 'employer_company_type']]
    return transformed_column
#print(transformed_column.head())

#Write to transformed_data_s3

#write the transformed data to csv
file = 'transformed_us_de_jobs.csv'
transformed_column.to_csv()

#Convert the list of dictionary into a dataframe

#Iterate through the list of dictionaries
# for dictionary in job_details:
#     #Access the individual dictionaries
#     for key, value in dictionary.items():
#         job_details_to_csv = 
        #print(f"{key}:{value}")
    
    # print("---")
#dataframe = df['data'][]

#print(deef)

#dataframe.to_csv('raw_de_usa_job.csv', index=False)




