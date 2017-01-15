__author__ = 'eliazarinelli'

import os
import sys
sys.path.insert(0, os.path.abspath('../..'))

from dana.models import Base, Orders

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import IntegrityError

import pickle
import time


DBSession = scoped_session(sessionmaker())
engine = None


def _init_sqlalchemy(engine_url):

    global engine
    engine = create_engine(engine_url, echo=False)
    DBSession.remove()
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)


def _clean_db():

    global engine

    # remove all tables in the database
    Base.metadata.drop_all(engine)

    # create all tables in the database defined in Base
    Base.metadata.create_all(engine)


def _read_orders(path_stage, name_pickle):

    list_orders = pickle.load(open('/'.join([path_stage, name_pickle]), "rb"))
    return list_orders


def _populate_db(list_orders):

    t_0 = time.time()

    try:
        engine.execute(Orders.__table__.insert(), list_orders)
        print(
            "Added " + str(len(list_orders)) +
            " records in " + str(time.time() - t_0) + " secs")

    except IntegrityError:
        print('At least one of the records that you are trying \n'
              'to insert is already present in the database. \n'
              'No new record written.')

if __name__ == '__main__':

    bool_clean_db = True

    path_stage = os.path.relpath('../../db/stage')

    name_pickle = 'example.p'

    db_path = os.path.abspath('../../db/db')
    if not os.path.exists(db_path):
        os.makedirs(db_path)

    db_name = 'example.db'
    engine_url = '/'.join(['sqlite:///', db_path, db_name])

    _init_sqlalchemy(engine_url)

    if bool_clean_db:
        _clean_db()
        print('Cleaned and initialised db in:')
        print(engine_url)

    list_orders = _read_orders(path_stage, name_pickle)

    _populate_db(list_orders)


