import requests
import json
import pandas as pd


url = "https://jsearch.p.rapidapi.com/search"

querystring = {"query":"Python developer in Texas, USA","page":"1","num_pages":"1"}

headers = {
	"X-RapidAPI-Key": "8dd97f9a7bmsha35d1101398dd53p11ea69jsnda918192f712",
	"X-RapidAPI-Host": "jsearch.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

#Fetch all data from api
data = response.json()

# print(new_data)

#This is Json pretty print, makes json output more readable
#json_str = json.dumps(data, indent=4)
#print(json_str)



# print(data)
# new_data = json.loads(data)
# print(new_data)
#print(response.status_code)








df = pd.json_normalize(data['data'])
df.to_csv('raw_data.csv', index=False)