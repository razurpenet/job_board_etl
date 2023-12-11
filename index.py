import requests
import json
import pandas as pd


url = "https://jsearch.p.rapidapi.com/search"

querystring = {"query":"Python developer in Texas, USA","page":"1","num_pages":"1"}

headers = {
	
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