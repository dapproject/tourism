import pandas.io.json
import requests
import json
from scripts import constant, logfile
from databases import mongodbConnection, sqldb
import os
import pandas as pd


class Flights(object):
    """
    The class is used to represent the flight details.

    Methods
    -------
    request_api()
        Api request to collect the information of flights runnning all over the world
    write_json(data)
        Write a json which store the information coming from api
    mongo_insert_details()
        Read json file and insert the records from json file to mongo db
    """

    def request_api(self, string, access_key, offset_range):
        """
        Run aviation api to collect the data of flights running all over the world
        :param string: type of string to pass in api url
        :param access_key: Access key for authentication
        :param offset_range: API offset
        :return: a list of records
        """
        data_list = []
        for single_offset in range(0, offset_range):
            offset = single_offset * 100
            params = {
                'access_key': access_key,
                'offset': offset,
                'limit': 100
            }
            json_response = requests.get('http://api.aviationstack.com/v1/' + string, params).json()
            data_list.append(json_response['data'])
        return data_list

    def collect_cities(self):
        # json_response = self.request_api('cities', constant.CITIES_ACCESS_KEY, 94)
        # self.write_json(json_response, constant.CITY_FILENAME)
        # self.mongo_insert_details(file_name=constant.CITY_FILENAME, collection_name=constant.MG_FLIGHT_CITIES_TABLE)
        data = self.get_mongo_flight_details(collection_name=constant.MG_FLIGHT_CITIES_TABLE)
        return self.cities_data_cleansing(data=data)

    def cities_data_cleansing(self, data):
        """
        Clean or remove unwanted details from the dataset
        :param data: Data in JSON format
        :return:
        """
        df = pd.DataFrame(data)
        new_df = pd.DataFrame()
        new_df[['country_iso2', 'city_name', 'iata_code']] = df[['country_iso2', 'city_name', 'iata_code']]

        # Drop na values
        new_df = new_df.dropna()
        return new_df

    def collect_countries(self):
        # json_response = self.request_api('countries', constant.CITIES_ACCESS_KEY, 3)
        # self.write_json(json_response, constant.COUNTRY_FILENAME)
        # self.mongo_insert_details(file_name=constant.COUNTRY_FILENAME,
        #                           collection_name=constant.MG_FLIGHT_COUNTRIES_TABLE)
        data = self.get_mongo_flight_details(collection_name=constant.MG_FLIGHT_COUNTRIES_TABLE)
        return self.countries_data_cleansing(data=data)

    def countries_data_cleansing(self, data):
        """
        Clean or remove unwanted details from the dataset
        :param data: Data in JSON format
        :return:
        """
        df = pd.DataFrame(data)
        new_df = pd.DataFrame()
        new_df[['country_iso2', 'country_name']] = df[['country_iso2', 'country_name']]
        # Drop na values
        new_df = new_df.dropna()
        return new_df

    def collect_data(self):
        # json_response = self.request_api('flights', constant.ACCESS_KEY,60)
        # self.write_json(json_response, constant.FLIGHTS_FILENAME)
        self.mongo_insert_details(file_name=constant.FLIGHTS_FILENAME, collection_name=constant.MG_FLIGHT_TABLE)

    def write_json(self, data, file_name):
        """
        Store the details in JSON file
        :param data: json response of aviation api
        :param file_name: name of file
        :return: write json file
        """
        json_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..',
                                      'jsonFiles/' + file_name + '.json')
        with open(json_file_path, 'w') as jsonFile:
            json.dump(data, jsonFile)

    def mongo_insert_details(self, file_name, collection_name):
        """
        Read data from JSON file and insert it to mongo db
        :param file_name: Name of File
        :return: details inserted in mongo db and close connection
        """
        mongo_conn = mongodbConnection.MongoDBConn()
        mongo_collection = mongo_conn.get_collection(collection_name)
        try:
            if mongo_collection.count() > 0:
                mongo_collection.drop()
                mongo_collection = mongo_conn.get_collection(collection_name)
            json_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..',
                                          'jsonFiles/' + file_name + '.json')
            with open(json_file_path) as jsonFile:
                json_obj_list = json.load(jsonFile)
                for json_obj in json_obj_list:
                    mongo_collection.insert_many(json_obj)
        except Exception as err:
            logfile.Log().log_error(err)
        finally:
            mongo_conn.close_conn()

    def get_mongo_flight_details(self, collection_name):
        """
        Get details from collection of mongo database
        :param collection_name: Name of collection
        :return:
        """
        mongo_conn = mongodbConnection.MongoDBConn()
        try:
            mongo_collection = mongo_conn.get_collection(collection_name)
            return mongo_collection.find({})
        except Exception as err:
            logfile.Log().log_error(err)
        finally:
            mongo_conn.close_conn()

    def merge_cities_countries(self):
        countries_df = self.collect_countries()
        cities_df = self.collect_cities()
        new_df = pd.merge(cities_df, countries_df, on='country_iso2')
        new_df = new_df.drop('country_iso2', axis=1)
        return new_df

    def data_cleansing(self, data):

        pd.set_option('display.max_columns', None)
        new_df = pd.DataFrame()
        primary_df = pd.DataFrame(data)
        # airline
        airline_df = pandas.io.json.json_normalize(primary_df['airline'])
        new_df['airline_name'] = airline_df['name']

        # flight_status
        new_df['flight_status'] = primary_df['flight_status']

        # Flight
        flight_df = pandas.io.json.json_normalize(primary_df['flight'])
        new_df['flight_number'] = flight_df['number']

        # Departure
        departure_df = pandas.io.json.json_normalize(primary_df['departure'])
        new_df[
            ['departure_delay', 'departure_airport', 'departure_scheduled', 'departure_timezone']] = departure_df[
            ['delay', 'airport', 'scheduled', 'timezone']]

        # Arrival
        arrival_df = pandas.io.json.json_normalize(primary_df['arrival'])
        new_df[['arrival_delay', 'arrival_airport', 'arrival_scheduled', 'arrival_timezone']] = arrival_df[
            ['delay', 'airport', 'scheduled', 'timezone']]

        sql_conn_obj = sqldb.SqlDBConn()
        sql_conn = sql_conn_obj.conn
        # cursor = sql_conn.cursor()

        new_df.to
        print(new_df)

        # print(df1.isnull().sum())
        # print(df1['aircraft'].loc[[4899]])
        # print(df.columns.values)
        # self.data_cleansing(details_obj)

#         column_names = ['airline_name', 'aircraft', 'flight_date', 'departure']
#         table_name = constant.MG_FLIGHT_TABLE
#
#         query_string = """CREATE TABLE flights_detail (
#     ID int NOT NULL,
#     LastName varchar(255) NOT NULL,
#     FirstName varchar(255),
#     Age int,
#     PRIMARY KEY (ID)
# ); """
#         for details in details_obj:
#             column_names.append(details['departure'])
#         sql_conn_obj = sqldb.SqlDBConn()
#         sql_conn = sql_conn_obj.conn
#         cursor = sql_conn.cursor()
#
#         cursor.execute("Drop table if exists " + table_name)
#         # cursor.execute("create table " + table_name)
#         cursor.execute(query_string)
#         sql_conn.commit()
#         sql_conn.close()
