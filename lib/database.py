from pymongo import MongoClient
from lib.helpers import log_exception
import sys
import logging


logging.basicConfig(format='%(asctime)s %(message)s',
                    filename='/var/log/syslog',
                    level=logging.INFO)


def database():
    try:
        server_ip = 'x.x.x.x'
        client = MongoClient(f'mongodb://{server_ip}:27017/')
        db = client.cca
        return db
    except Exception as e:
        logging.info(f'Failed to connect to db, erro = {e}')
    return


def drop_database(database_name):
    try:
        server_ip = 'x.x.x.x'
        client = MongoClient(f'mongodb://{server_ip}:27017/')
        res = client.drop_database(database_name)
    except Exception as e:
        logging.info(f'Failed to drop db, erro = {e}')
        res = 'fail'
    return res


# inserts
def insert_one(collection, data, db=False):
    retry = 5
    count = 0
    success = False
    try:
        if db is False:
            db = database()
        while count <= retry:
            count += 1
            res = db[collection].insert_one(data)
            if res.acknowledged is True:
                count = 6
                success = True
    except Exception as e:
        logging.info(f'Failed to insert, error = {e}')
    if success is False:
        res = 'fail'
        msg = 'too many retries'
        logging.info(f'Failed to insert, error = {msg}')
    return res


def insert_many(collection, data, db=False):
    retry = 5
    count = 0
    success = False
    try:
        if db is False:
            db = database()

        while count <= retry:
            count += 1
            res = db[collection].insert_many(data)
            if res.acknowledged is true:
                count = 6
                success = True
    except Exception as e:
        logging.info(f'Failed to insert many, error = {e}')
    if success is False:
        res = 'fail'
        msg = 'too many retries'
        logging.info(f'Failed to insert many, error = {msg}')
    return res


# finds
def find_all(collection, db=False):
    try:
        if db is False:
            db = database()
        return [data for data in db[collection].find()]
    except Exception as e:
        logging.info(f'Failed to find_all, error = {e}')
        return


def find_filter(collection, find_filter, db=False):
    try:
        if db is False:
            db = database()
        return [data for data in db[collection].find(find_filter)]
    except Exception as e:
        logging.info(f'Failed to find_all, error = {e}')
        return


def find_filter_partial(collection, find_filter, fields, db=False):
    try:
        if db is False:
            db = database()
        return [data for data in db[collection].find(find_filter, fields)]
    except Exception as e:
        logging.info(f'Failed to find_all, error = {e}')
        return


def find_limit(collection, find_limit, db=False):
    try:
        if db is False:
            db = database()
        return [data for data in db[collection].find().limit(find_limit)]
    except Exception as e:
        logging.info(f'Failed to find_all, error = {e}')
        return


def find_filter_limit(collection, find_filter, find_limit, db=False):
    try:
        if db is False:
            db = database()
        return [data for data in
                db[collection].find(find_filter).limit(find_limit)]
    except Exception as e:
        logging.info(f'Failed to find_filter_limit, error = {e}')
        logging.info(f'{collection}, {find_filter}, {find_limit}')
        return


def find_filter_limit_sort(collection,
                           find_filter,
                           find_limit,
                           sort,
                           db=False):
    try:
        if db is False:
            db = database()
        res = [data for data in
               db[collection].find(find_filter).limit(find_limit).sort(sort)]
    except Exception as e:
        log_exception(sys.argv[0], '# find_filter_limit_sort', e)
        res = 'error'
    return res


def count_filter(collection, find_filter, db=False):
    try:
        if db is False:
            db = database()
        return db[collection].count_documents(find_filter)
    except Exception as e:
        msg = f'Failed to get news_article_collector, error = {e}'
        logging.info(msg)
        return


# updates
def update_one(collection, match, data, db=False):
    try:
        if db is False:
            db = database()
        res = db[collection].update_one(match, {'$set': data})
    except Exception as e:
        logging.info(f'Failed to update_one, error = {e}')
        msg = f'col {collection}, match {match}, data {data}'
        logging.info(msg)
        res = 'error'
    return res


# delete
def delete_one(collection, match):
    try:
        db = database()
        res = db[collection].delete_one(match)
    except Exception as e:
        logging.info(f'Failed to delete_one, error = {e}')
        res = 'fail'
    return res


def delete_many(collection, match):
    try:
        db = database()
        res = db[collection].delete_many(match)
    except Exception as e:
        logging.info(f'Failed to delete_many, error = {e}')
        res = 'fail'
    return res

# other
def aggregate(collection, _filter, _match):
    db = database()
    return db[collection].aggregate([_filter, _match])

