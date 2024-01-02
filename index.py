import requests
import json
import pandas as pd
import configparser
import os
import boto3
from datetime import datetime
from io import StringIO
from utils.helper import does_bucket_exists, create_bucket, create_transformed_jobs_bucket


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

#Create s3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secretkey, region_name=aws_region)

# #create s3 bucket
if does_bucket_exists(aws_bucketname):
    pass
else:
    create_bucket()
    #print(f'Bucket-{aws_bucketname} created successfully')



#Write raw data to s3 bucket
bucketname = aws_bucketname
# local_folder = './json_folder'
# s3_prefix = 'jobboard101/'

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


#TRANSFORMATION - Read from raw data in s3 bucket -jobboard101

def write_to_transformed_s3():
    if does_bucket_exists(aws_transformedbucket):
        pass
    else:
        create_transformed_jobs_bucket()
    #print(f'Bucket-{aws_transformedbucket} created successfully'))
    
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

    #Select required column from dataframe
    transformed_column = df[['employer_website', 'job_id', 'job_title', 'job_apply_link', 
                            'job_description', 'job_city', 'job_country', 
                            'job_posted_at_timestamp', 'employer_company_type']]
    #write the transformed data to csv
    #transformed_col = transformed_column.to_csv('transformed_us_de_jobs.csv', index=False)
    #Data Cleaning
    df = transformed_column.copy()
    new_transformed = df.drop_duplicates()
    #Check Datatype pf the columns
    #new_transformed = df.info()
    #Dealing with timestamp (UNIX)
    new_transformed['job_posted_at_timestamp'] = (new_transformed['job_posted_at_timestamp']
                                                  .convert_dtypes(convert_integer=True)).astype(int)
    new_transformed['job_posted_at_timestamp'] = pd.to_datetime(new_transformed['job_posted_at_timestamp'], unit='s')

    print(new_transformed)

write_to_transformed_s3()
    



#This creates the transformed-jobs-data s3 bucket 


        

#
    
    #create_transformed_jobs_bucket()






