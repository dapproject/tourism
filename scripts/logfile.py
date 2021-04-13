import logging
import os


class Log(object):
    def __init__(self):
        file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'logfile.log')
        logging.basicConfig(filename=file_path, format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s')

    def log_error(self, err):
        print(err)
        logging.error(err)

    def log_info(self, err):
        logging.info(err)
