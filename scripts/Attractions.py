from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import os
from databases import mongodbConnection, sqldb
from scripts import constant, logfile
import plotly.express as plt


class Attractions(object):

    def scraping(self):
        json_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..',
                                      'jsonFiles/Tripadvisor_Links.csv')
        Links = pd.read_csv(json_file_path, header=None)
        Result = []
        for Link in Links:
            print(Link)
            r = requests.get(Link)
            soup = bs(r.content, "html.parser")
            type(soup)
            data_dict = {"City": [], "Places": [], "Address": [], "Website": [], "Phone": [], "Reviews": [],
                         "Rating (Out of 5)": []}
            title_elem = soup.find('h1', class_='ui_header h1')
            print(title_elem.text)
            data_dict["Places"] = (title_elem.text.strip())
            address_elem = soup.find('div', class_='LjCWTZdN')
            data_dict["Address"] = (address_elem.text.strip())
            website_elem = soup.find('div', class_='_1ev9TQ-P')
            data_dict["Website"] = (website_elem.a['href'])
            phone_elem = soup.find('a', class_='_TF8HH3_')
            data_dict["Phone"] = (phone_elem.text.strip())
            Review_elem = soup.find('div', class_='_1NKYRldB')
            data_dict["Reviews"] = (Review_elem.text.strip())
            City_elem = soup.find('div', class_='eQSJNhO6')
            City = City_elem.text.strip()
            data_dict["City"] = (City.split(" ", 7)[-1])
            Rating_elem = soup.find('a', class_='_1d_R5B7y')
            try:
                data_dict["Rating (Out of 5)"] = (Rating_elem.text.strip())
            except:
                data_dict["Rating (Out of 5)"] = (Rating_elem)
            Result.append(data_dict)

        return Result

    def insert_mongo(self):
        data = self.scraping()
        conn = mongodbConnection.MongoDBConn()
        attraction_collection = conn.get_collection(constant.MG_ATTRACTIONS_TABLE)
        try:
            attraction_collection.insert_many(data.to_dict('records'))
        except Exception as err:
            print(err)
        finally:
            conn.close_conn()

    def get_details_mongo(self):
        # self.insert_mongo()
        conn = mongodbConnection.MongoDBConn()
        attraction_collection = conn.get_collection(constant.MG_ATTRACTIONS_TABLE)
        try:
            return attraction_collection.find({})
        except Exception as err:
            logfile.Log().log_error(err)
        finally:
            conn.close_conn()

    def data_cleaning(self):
        data = self.get_details_mongo()
        df = pd.DataFrame(list(data))
        df.drop(['_id'], axis='columns', inplace=True)
        df.head(520)
        df[df[['Places']].duplicated() == True]

        # obj.close_conn()
        #
        # client = mo
        # db = client['tourism']
        # col = db['attractions']

        sql_conn = sqldb.SqlDBConn().conn

        cursor = sql_conn.cursor()
        try:
            cursor.execute('''DROP TABLE dbo.Attractions''')
            cursor.execute(
                '''CREATE TABLE Attractions ( City nvarchar(200) , Places nvarchar(200) , Address nvarchar(200) , Website nvarchar(200),Phone nvarchar(200) , Rating nvarchar(200) , Review nvarchar(200))''')
            for index, row in df.iterrows():
                cursor.execute(
                    "INSERT INTO Attractions (City,Places,Address,Website,Phone,Rating,Review) values(?,?,?,?,?,?,?)",
                    row.City,
                    row.Places, row.Address, row.Website, row.Phone, row.Rating, row.Review)
            sql_conn.commit()
        except Exception as err:
            logfile.Log().log_error(err)
        finally:
            cursor.close()

        sql_conn = sqldb.SqlDBConn().conn
        output_df = pd.DataFrame(
            pd.read_sql("SELECT City,Avg(Review) as Avg_Review from dbo.attractions GROUP By City", sql_conn))

        fig = plt.bar(output_df, y='Avg_Review', x='City',
                      labels={'Avg_Review': 'Avg Review', 'City': 'city'})
        fig.show()

    def flight_landing(self):
        sql_conn = sqldb.SqlDBConn().conn
        database_result = pd.read_sql(
            "select Distinct aa.* from flights_detail fd INNER JOIN cities_countries cc on fd.arrival_city_id=cc.city_id INNER 	JOIN Attractions aa on aa.City=cc.city where flight_status='landed' ORDER by Review DESC",
            sql_conn)
        print(database_result)
