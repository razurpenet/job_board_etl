import requests
import json
import pandas as pd
import configparser
import boto3
from utils.helper import create_bucket()


config = configparser.ConfigParser()

config.read('.env')
api_access = config['API_ACCESS']['X-RapidAPI-Key']
api_host = config['API_ACCESS']['X-RapidAPI-Host']

aws_access = config['AWS_ACCESS']['access']
aws_secretkey = config['AWS_ACCESS']['secret_key']

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
    with open("raw_de_us_jobs.json", "w") as file:
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
    with open("raw_de_uk_jobs.json", "w") as file:
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

    with open("raw_de_ca_jobs.json", "w") as file:
        json.dump(data, file)
    

    # df = pd.json_normalize(data['data'])
    # print(df)

extract_de_ca_jobs()

#create s3 bucket

create_bucket()


#Write raw data to csv
