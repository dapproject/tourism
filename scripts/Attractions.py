from bs4 import BeautifulSoup as bs
import requests
import json
import pandas as pd
import pymongo
from pymongo import MongoClient
client = pymongo.MongoClient("mongodb+srv://dapGroup1:dapGroup123@tourism.wurzf.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client['tourism']
col = db['attractions']

Links = pd.read_csv("C:\\Users\\Utkarsh Mathur\\Desktop\\Tripadvisor_Links.csv", header = None)
Links = list(Links[0])
Links.remove('Links')

Links
print(len(list(Links)))

Result = []
for Link in Links:
    print (Link)
    r = requests.get(Link)
    soup = bs(r.content, "html.parser")
    type(soup)
    data_dict={"City":[],"Places":[],"Address":[],"Website":[],"Phone":[],"Reviews":[],"Rating (Out of 5)":[]}
    title_elem = soup.find('h1',class_='ui_header h1')
    print (title_elem.text)
    data_dict["Places"]=(title_elem.text.strip())
    address_elem = soup.find('div',class_='LjCWTZdN')
    data_dict["Address"]=(address_elem.text.strip())
    website_elem = soup.find('div',class_='_1ev9TQ-P')
    data_dict["Website"]=(website_elem.a['href'])
    phone_elem = soup.find('a',class_='_TF8HH3_')
    data_dict["Phone"]=(phone_elem.text.strip())
    Review_elem = soup.find('div',class_='_1NKYRldB')
    data_dict["Reviews"]=(Review_elem.text.strip())
    City_elem = soup.find('div',class_='eQSJNhO6')
    City = City_elem.text.strip()
    data_dict["City"]=(City.split(" ",7)[-1])
    Rating_elem = soup.find('a',class_='_1d_R5B7y')
    try:
        data_dict["Rating (Out of 5)"]=(Rating_elem.text.strip())
    except:
        data_dict["Rating (Out of 5)"]=(Rating_elem)
    Result.append(data_dict)
    
final=pd.DataFrame().from_dict(Result)

Result

mongo_dataset = col.insert_many(final.to_dict('records'))
mongo_dataset

df = pd.DataFrame(list(col.find()))
df.drop(['_id'], axis='columns', inplace=True)
df.head(520)

df[df[['Places']].duplicated() == True]