import configparser as cp
import os

config = cp.ConfigParser()
config_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'config.cfg')
config.read(config_file_path)

ACCESS_KEY = config.get('flights', 'accesskey')
MONGO_USERNAME = config.get('mongodb', 'username')
MONGO_PASSWORD = config.get('mongodb', 'password')
MG_DB_NAME = 'tourism'
MG_FLIGHT_TABLE = 'flights_detail'
