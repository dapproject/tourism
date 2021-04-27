# Script Created by Rohit Yadav

import pyodbc
from scripts import constant, logfile


class SqlDBConn(object):
    """
    The class is used to represent the work flow of relation database connection
    Create database connection in default constructor

    Methods
    -------
    close_conn()
        close the mongodb connection
    get_collection(table_name)
        Get the collection object of database from mongo db
    """

    def __init__(self):
        """
        Establish the database connection and if it fails to create the connection then log the error in log file
        """

        conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + constant.SQL_SERVER + ';DATABASE=' + constant.SQL_DB_NAME + ';UID=' + constant.SQL_USERNAME + ';PWD=' + constant.SQL_PASSWORD
        try:
            self.conn = pyodbc.connect(conn_string)
        except Exception as err:
            logfile.Log().log_error(err)

    def close_conn(self):
        """
        Close database connection
        :return: closed connection object
        """
        self.conn.close()
