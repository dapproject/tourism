# Script Created by Rohit Yadav

import numpy as np
import pandas.io.json
import requests
import json
from scripts import constant, logfile
from databases import mongodbConnection, sqldb
import os
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px


class Flights(object):
    """
    The class is used to represent the flight details.

    Methods
    -------
    request_api()
        Api request to collect the information of flights running all over the world
    collect_cities ()
        Collect cities list from api. The date will store it mongo db and after data cleaning it will store in MSSQL
    cities_data_cleansing (data)
        Clean cities data
    collect_countries ()
        Collect countries list from api. The data will stored it mongo db and after data cleaning it will store in MSSQL
    countries_data_cleansing (data)
        Clean Countries data
    collect_data ()
        Collect flight data from api. The data will stored it mongo db and after data cleaning it will store in MSSQL
    write_json (data, file_name)
        Write a json and save it with filename parameter
    mongo_insert_details(file_name, collection_name)
        Read json file and insert the records from json file to mongo db
    get_mongo_flight_details(collection_name)
        Retrieve data of the collection from mongo
    merge_cities_countries()
        Merge cities and countries data in dataframe
    insert_cities_countries()
        insert cities and countries in MSSQL database
    data_cleansing(data)
        Clean or remove unwanted details from the dataset and stored the cleaned data in MSSQL
    schedule_graph()
        Generate the map graph which shows the count of scheduled flights
    flight_status()
        Generate the bar graph which shows the count of all flight statuses
    arrival_delay()
        Generate the pie chart which shows the average arrival delay of all flights
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
        json_response = self.request_api('cities', constant.CITIES_ACCESS_KEY, 94)
        self.write_json(json_response, constant.CITY_FILENAME)
        self.mongo_insert_details(file_name=constant.CITY_FILENAME, collection_name=constant.MG_FLIGHT_CITIES_TABLE)
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
        # Drop duplicates
        new_df = new_df.drop_duplicates()

        return new_df

    def collect_countries(self):
        """
        Request api to collect countries data and store it in JSON file. Call methods to store data in mongo db and clean the dataset
        :return: dataframe
        """
        json_response = self.request_api('countries', constant.CITIES_ACCESS_KEY, 3)
        self.write_json(json_response, constant.COUNTRY_FILENAME)
        self.mongo_insert_details(file_name=constant.COUNTRY_FILENAME,
                                  collection_name=constant.MG_FLIGHT_COUNTRIES_TABLE)
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
        # Drop duplicates
        new_df = new_df.drop_duplicates()
        return new_df

    def collect_data(self):
        """
        Request api to collect flights data and store it in JSON file. Call methods to store data in mongo db and clean the dataset
        After clean store it in mongo db
        :return:
        """
        json_response = self.request_api('flights', constant.ACCESS_KEY, 100)
        self.write_json(json_response, constant.FLIGHTS_FILENAME)
        self.mongo_insert_details(file_name=constant.FLIGHTS_FILENAME, collection_name=constant.MG_FLIGHT_TABLE)
        data = self.get_mongo_flight_details(collection_name=constant.MG_FLIGHT_TABLE)
        self.data_cleansing(data=data)

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
        """
        Merge cities and countries dataset
        :return: dataframe
        """
        countries_df = self.collect_countries()
        cities_df = self.collect_cities()
        return pd.merge(cities_df, countries_df, on='country_iso2')

    def insert_cities_countries(self):
        """
        Insert cities and countries in MSSQL database
        :return:
        """
        df = self.merge_cities_countries()
        df = df.drop('country_iso2', axis=1)
        df = df.drop('iata_code', axis=1)
        cities_list = [tuple(rows) for rows in df.values]
        sql_conn = sqldb.SqlDBConn().conn
        cursor = sql_conn.cursor()
        try:
            drop_city_table = 'drop table if exists ' + constant.CITIES_TABLE
            create_city_table = 'Create table ' + constant.CITIES_TABLE + ' (city_id int NOT NULL IDENTITY(1,1), city varchar(255) NOT NULL, country varchar(255) NOT NULL, PRIMARY KEY(city_id))'
            insert_city_sql = 'insert into ' + constant.CITIES_TABLE + ' (city,country) values (?, ?)'
            cursor.execute(drop_city_table)
            cursor.execute(create_city_table)
            cursor.executemany(insert_city_sql, cities_list)
            sql_conn.commit()
        except Exception as err:
            sql_conn.rollback()
            logfile.Log().log_error(err)
        finally:
            sql_conn.close()

    def data_cleansing(self, data):
        """
        Clean flights dataset and store it in MSSQL
        :param data: dataframe
        :return:
        """
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
        new_df[['flight_number', 'flight_iata']] = flight_df[['number', 'iata']]

        # Departure
        departure_df = pandas.io.json.json_normalize(primary_df['departure'])
        new_df[
            ['departure_delay', 'departure_airport', 'departure_scheduled', 'departure_iata']] = departure_df[
            ['delay', 'airport', 'scheduled', 'iata']]

        # Arrival
        arrival_df = pandas.io.json.json_normalize(primary_df['arrival'])
        new_df[['arrival_delay', 'arrival_airport', 'arrival_scheduled', 'arrival_iata']] = arrival_df[
            ['delay', 'airport', 'scheduled', 'iata']]

        cities_df = self.merge_cities_countries()

        new_df = pd.merge(new_df, cities_df, left_on='departure_iata', right_on='iata_code')
        new_df = new_df.rename({'city_name': 'departure_city'}, axis=1)

        new_df = pd.merge(new_df, cities_df, left_on='arrival_iata', right_on='iata_code')
        new_df = new_df.rename({'city_name': 'arrival_city'}, axis=1)

        sql_conn = sqldb.SqlDBConn().conn
        cursor = sql_conn.cursor()

        city_table_df = pd.read_sql("select * from " + constant.CITIES_TABLE, con=sql_conn)

        new_df = pd.merge(new_df, city_table_df, left_on='departure_city', right_on='city')
        new_df = new_df.rename({'city_id': 'departure_city_id'}, axis=1)

        new_df = pd.merge(new_df, city_table_df, left_on='arrival_city', right_on='city')
        new_df = new_df.rename({'city_id': 'arrival_city_id'}, axis=1)

        new_df = new_df.drop(
            ['departure_iata', 'arrival_iata', 'country_iso2_x', 'iata_code_x', 'country_name_x', 'country_iso2_y',
             'iata_code_y', 'country_name_y', 'departure_city', 'arrival_city', 'city_x', 'country_x', 'city_y',
             'country_y'], axis=1)

        new_df = new_df.replace({np.NaN: None})
        new_df['departure_delay'] = new_df['departure_delay'].replace({None: 0})
        new_df['arrival_delay'] = new_df['arrival_delay'].replace({None: 0})

        drop_flight_table = 'drop table if exists ' + constant.MG_FLIGHT_TABLE
        flight_table_sql = 'create table ' + constant.MG_FLIGHT_TABLE + ' (id int not null IDENTITY(1,1), airline_name varchar(255), ' \
                                                                        'flight_status varchar(255), flight_number varchar(255), flight_iata varchar(255), ' \
                                                                        'departure_delay decimal, departure_airport varchar(255), departure_scheduled varchar(255), ' \
                                                                        'arrival_delay decimal, arrival_airport varchar(255), arrival_scheduled varchar(255),' \
                                                                        ' departure_city_id int, arrival_city_id int, PRIMARY KEY (id), FOREIGN KEY (arrival_city_id) REFERENCES ' + constant.CITIES_TABLE + ' (city_id), FOREIGN KEY (departure_city_id) REFERENCES ' + constant.CITIES_TABLE + ' (city_id))'

        column_name_list = []
        for col in new_df.columns:
            column_name_list.append(col)
        data_list = [tuple(rows) for rows in new_df.values]
        insert_sql = "INSERT INTO " + constant.MG_FLIGHT_TABLE + "(" + ', '.join(
            column_name_list) + ") VALUES (" + "?," * (
                             len(column_name_list) - 1) + "?)"

        try:
            cursor.execute(drop_flight_table)
            cursor.execute(flight_table_sql)
            cursor.executemany(insert_sql, data_list)
            sql_conn.commit()
        except Exception as err:
            logfile.Log().log_error(err)
            sql_conn.rollback()
        finally:
            sql_conn.close()

    def schedule_graph(self):
        """
        Create the map graph which shows the count of scheduled flights
        :return: boolean
        """
        sql_conn = sqldb.SqlDBConn().conn
        try:
            flight_status_df = pd.read_sql(
                "SELECT country,count(country) as c_count FROM flights_detail fd INNER JOIN cities_countries cc on fd.departure_city_id=cc.city_id where flight_status='scheduled' GROUP by cc.country ORDER BY count(country) DESC",
                con=sql_conn)
            fig = px.choropleth(flight_status_df, locations=flight_status_df['country'], locationmode='country names',
                                color='c_count', labels={'c_count': 'Number of Scheduled Flights'},
                                color_continuous_scale='Inferno')
            fig.show(renderer="browser")
        except Exception as err:
            logfile.Log().log_error(err)
        return True

    def flight_status(self):
        """
        Create the bar graph which shows the count of all flight statuses
        :return: boolean
        """
        sql_conn = sqldb.SqlDBConn().conn
        try:
            flight_status_df = pd.read_sql(
                "SELECT flight_status,count(flight_status) as s_count from flights_detail GROUP by flight_status",
                con=sql_conn)
            fig = px.bar(flight_status_df, y='s_count', x='flight_status',
                         labels={'flight_status': 'Flight Status', 's_count': 'Count'})
            fig.show()
        except Exception as err:
            logfile.Log().log_error(err)
        return True

    def arrival_delay(self):
        """
        Create the pie chart which shows the average arrival delay of all flights
        :return: boolean
        """
        sql_conn = sqldb.SqlDBConn().conn
        try:
            flight_status_df = pd.read_sql(
                "SELECT AVG(arrival_delay) as mean, arrival_airport from flights_detail fd inner join cities_countries cc on fd.arrival_city_id=cc.city_id where arrival_delay<>0 GROUP by arrival_airport,city",
                con=sql_conn)
            fig = px.pie(flight_status_df, values='mean', names='arrival_airport',
                         labels={'arrival_airport': 'Arrival Airport name', 'mean': 'Average delay'})
            fig.show()
        except Exception as err:
            logfile.Log().log_error(err)
        return True

    def common_insight(self):
        sql_conn = sqldb.SqlDBConn().conn
        try:
            result_df = pd.read_sql(
                "select Review,Places,City from attractions where City in (select Top 1 cli.City from flights_detail fd inner join cities_countries cc on fd.arrival_city_id=cc.city_id INNER JOIN city_living_index cli on cc.city=cli.City INNER JOIN attractions aa on aa.City=cc.city where flight_status='scheduled' GROUP by cli.city, flight_status order by AVG([Rent Index]) ASC) and Ratings is not null order by Review Desc",
                con=sql_conn)
            city = result_df['City'].iloc[0]
            fig = px.pie(result_df, values='Review', names='Places',
                         labels={'Places': 'Places', 'Review': 'Review'}, title='Top 10 Places and reviews in ' + city)
            fig.show()
        except Exception as err:
            logfile.Log().log_error(err)
        return True
