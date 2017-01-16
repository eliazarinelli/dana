__author__ = 'eliazarinelli'

import os
import sys
sys.path.insert(0, os.path.abspath('../..'))

from dana.models import Base, Orders

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

import pickle
import time


def _init_sqlalchemy(engine_url_in):

    """ Create an instance of the engine specified by the input url """

    return create_engine(engine_url_in, echo=False)


def _clean_db(engine_in):

    """ Drop the tables of the Base class and create new ones"""

    # remove all tables in the database
    Base.metadata.drop_all(engine_in)

    # create all tables in the database defined in Base
    Base.metadata.create_all(engine_in)


def _read_orders(path_stage, name_pickle):

    """ Read the orders stored in a pickle file """

    return pickle.load(open('/'.join([path_stage, name_pickle]), "rb"))


def _populate_db(engine_in, list_orders_in):

    """ Populate the database with the input orders """

    t_0 = time.time()

    try:
        engine_in.execute(Orders.__table__.insert(), list_orders_in)
        print(
            "Added " + str(len(list_orders_in)) +
            " records in " + str(time.time() - t_0) + " secs")

    except IntegrityError:
        print('At least one of the records that you are trying \n'
              'to insert is already present in the database. \n'
              'No new record written.')

if __name__ == '__main__':

    # database configurations
    db_path = os.path.abspath('../../db/db')
    db_name = 'example.db'
    engine_url = '/'.join(['sqlite:///', db_path, db_name])

    # create the folder where the db is stored
    if not os.path.exists(db_path):
        os.makedirs(db_path)

    # create the engine
    engine = _init_sqlalchemy(engine_url)

    # clean the db
    # set the flag to True if you want to clean
    bool_clean_db = True
    if bool_clean_db:
        _clean_db(engine)
        print('Cleaned and initialised db in:')
        print(engine_url)

    # pickle configurations
    path_stage = os.path.relpath('../../db/stage')
    name_pickle = 'example.p'

    # read the orders in the pickle file and store in the db
    list_orders = _read_orders(path_stage, name_pickle)
    _populate_db(engine, list_orders)


