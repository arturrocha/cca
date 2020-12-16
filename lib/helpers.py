import logging
import time


logging.basicConfig(format='%(asctime)s %(message)s',
                    filename='/var/log/syslog',
                    level=logging.INFO)


def log_exception(file_name, section, e):
    logging.info("Exception caught at file = {0}".format(file_name))
    logging.info("Section = {0}".format(section))
    logging.info("Error = {0}".format(e))
    return


def log_message(message):
    logging.info("{0}".format(message))
    return


def get_human_date(ts):
    ts_format = '%Y-%m-%d %H:%M:%S'
    return time.strftime(ts_format, time.localtime(ts))
