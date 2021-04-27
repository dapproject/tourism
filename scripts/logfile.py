# Script Created by Rohit Yadav

import logging
import os


class Log(object):
    """
    The class represents the use of logs. Store logs of scripts in log file
    """

    def __init__(self):
        """
        Define path of log file
        """
        file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'logfile.log')
        logging.basicConfig(filename=file_path, format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s')

    def log_error(self, err):
        """
        Log error in log file
        :param err:
        :return:
        """
        print(err)
        logging.error(err)

    def log_info(self, err):
        """
        log info in log file
        :param err:
        :return:
        """
        logging.info(err)
