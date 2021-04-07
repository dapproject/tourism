import urllib
import pymongo
from scripts import constant
import logging


class MongoDBConn(object):
    """
    The class is used to represent the work flow of mongo db connection
    Create mongo db connection in default constructor

    Methods
    -------
    close_conn()
        close the mongodb connection
    get_collection(table_name)
        Get the collection object of database from mongo db
    """

    def __init__(self):
        """
        Establish the mongo db connection and if it fails to create the connection then log the error in log file
        """
        logging.basicConfig(filename='./../logfile.log', format='%(asctime)s - %(levelname)s - %(message)s',
                            level=logging.ERROR)
        # client = pymongo.MongoClient()
        self.url = 'mongodb+srv://' + constant.MONGO_USERNAME + ':' + urllib.parse.quote_plus(
            constant.MONGO_PASSWORD) + '@' + constant.MG_DB_NAME + '.wurzf.mongodb.net/test'
        try:
            self.conn = pymongo.MongoClient(self.url)
            self.conn.server_info()
        except Exception as err:
            logging.error(err)

    def close_conn(self):
        """
        Close mongo db connection
        :return: closed connection object
        """
        self.conn.close()

    def get_collection(self, table_name):
        """
        Get the collection object from mongo db
        :param table_name: name of the collection
        :return: collection object
        """
        mongo_db = self.conn[constant.MG_DB_NAME]
        return mongo_db[table_name]
