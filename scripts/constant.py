import configparser as cp
import os

config = cp.ConfigParser()
config_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'config.cfg')
config.read(config_file_path)

ACCESS_KEY = config.get('flights', 'accesskey')
CITIES_ACCESS_KEY = config.get('flights', 'citiesaccesskey')
MONGO_USERNAME = config.get('mongodb', 'username')
MONGO_PASSWORD = config.get('mongodb', 'password')
SQL_USERNAME = config.get('sql', 'username')
SQL_PASSWORD = config.get('sql', 'password')
MG_DB_NAME = 'tourism'
MG_FLIGHT_TABLE = 'flights_detail'
MG_FLIGHT_CITIES_TABLE = 'cities'
MG_FLIGHT_COUNTRIES_TABLE = 'countries'
CITY_FILENAME = 'cities'
COUNTRY_FILENAME = 'countries'
FLIGHTS_FILENAME = 'flightsapi'
SQL_SERVER = 'localhost'
CITIES_TABLE = 'cities_countries'
