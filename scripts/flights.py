import requests
import json
from scripts import constant
from databases import mongodbConnection
import os


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

    def request_api(self):
        """
        Run aviation api to collect the data of flights running all over the world
        :return: a list of records
        """
        data_list = []
        for offsetRange in range(1, 50):
            offset = offsetRange * 100
            params = {
                'access_key': constant.ACCESS_KEY,
                'offset': offset,
                'limit': 100
            }
            json_response = requests.get('http://api.aviationstack.com/v1/flights', params).json()
            data_list.append(json_response['data'])
        return data_list

    def write_json(self, data):
        """
        Store the details in JSON file
        :param data: json response of aviation api
        :return: write json file
        """
        json_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'jsonFiles/flightsapi.json')
        with open(json_file_path, 'w') as jsonFile:
            json.dump(data, jsonFile)

    def mongo_insert_details(self):
        """
        Read data from JSON file and insert it to mongo db
        :return: details inserted in mongo db and close connection
        """
        mongo_conn = mongodbConnection.MongoDBConn()
        mongo_collection = mongo_conn.get_collection(constant.MG_FLIGHT_TABLE)
        if mongo_collection.count() > 0:
            mongo_collection.drop()
            mongo_collection = mongo_conn.get_collection(constant.MG_FLIGHT_TABLE)
        json_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'jsonFiles/flightsapi.json')
        with open(json_file_path) as jsonFile:
            json_obj_list = json.load(jsonFile)
            for json_obj in json_obj_list:
                mongo_collection.insert_many(json_obj)
                mongo_conn.close_conn()
                exit()
        # mongo_conn.close_conn()
