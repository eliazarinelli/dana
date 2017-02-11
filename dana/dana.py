from .userconfig import *

from .models import Orders
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from sqlalchemy import func

from pymongo import MongoClient

N_MINS_DAY = 24*60

class Dana(object):

    def __init__(self, engine_url):

        engine = create_engine(engine_url)
        Session = sessionmaker()
        Session.configure(bind=engine)
        self._session = Session()

    def count_orders(self, symbol=None):

        if symbol is None:
            return self._session.query(Orders.mgr).count()
        else:
            return self._session.query(Orders.mgr).filter(Orders.symbol==symbol).count()

    def get_day(self, symbol=None, date=None):

        """
        Return the orders executed on the input date and symbol.
        The output is a list of dicts, each dict contains the data of an oders.
        """

        output = []

        record_list = self._session.query(Orders).\
            filter(and_(Orders.symbol == symbol, Orders.date_exec == date))

        for record in record_list:
            order = {
                'mgr': record.mgr,
                'bkr': record.bkr,
                'symbol': record.symbol,
                'side': record.side,
                'date_exec': record.date_exec,
                'time_start': record.time_start,
                'time_end': record.time_end,
                'volume': record.volume,
                'price': record.price,
                'n_trades': record.n_trades
            }

            output.append(order)

        return output

    def get_symbols(self):

        """ List of the distinct symbols in the db """

        tmp = [i[0] for i in self._session.query(Orders.symbol).distinct()]
        tmp.sort()
        return tmp

    def get_dates(self):

        """ List of the distinct dates in the db """

        tmp = [i[0] for i in self._session.query(Orders.date_exec).distinct()]
        tmp.sort()
        return tmp

TS_FIELDS = set(['date', 'mins', 'price', 'volume'])
ORDERS_FIELDS = set(['mgr', 'bkr', 'symbol', 'side', 'date',
                     'min_start', 'min_end', 'volume', 'price', 'ntrades'])


class Hts(object):

    def __init__(self, engine_url, db_ts_name, db_orders_name):

        self._db_ts_name = db_ts_name
        self._db_orders_name = db_orders_name
        self._client = MongoClient(engine_url)
        self._db_ts = self._client[db_ts_name]
        self._db_orders = self._client[db_orders_name]

    def drop_ts(self, symbol=None):

        if symbol is None:
            self._client.drop_database(self._db_ts_name)
        else:
            self._db_ts.drop_collection(symbol)

    def insert_ts(self, symbol, ts):

        if set(ts[0].keys()) != TS_FIELDS:
            raise ValueError('Wrong format of input in ts: dict keys should be: ' + ', '.join(TS_FIELDS))

        c_symbol = self._db_ts.get_collection(symbol)
        c_symbol.insert_many(ts)

    def get_ts(self, symbol, date, start=0, end=N_MINS_DAY):

        c_symbol = self._db_ts[symbol]

        cc = c_symbol.find({'date': date, 'mins': {'$gte': start, '$lt': end}}, {"_id": 0}).sort([('mins', 1)])
        output = [i for i in cc]
        return output

    def get_candle(self, symbol, date, start=0, end=N_MINS_DAY):

        ts = self.get_ts(symbol, date, start, end)

        output = {
            'open': ts[0]['price'],
            'high': max([i['price'] for i in ts]),
            'low': min([i['price'] for i in ts]),
            'close': ts[-1]['price'],
            'volume': sum([i['volume'] for i in ts]),
            'vwap': sum([i['volume']*i['price'] for i in ts])/sum([i['volume'] for i in ts])
        }
        return output

    def drop_orders(self):

        self._client.drop_database(self._db_orders_name)

    def insert_orders(self, orders_in):

        if set(orders_in[0].keys()) != ORDERS_FIELDS:
            raise ValueError('Wrong format of input in orders: dict keys should be: ' + ', '.join(ORDERS_FIELDS))

        c_orders = self._db_orders.get_collection('orders')
        c_orders.insert_many(orders_in)

    def get_orders(self, symbol, date, start_min=0, start_max=N_MINS_DAY, end_min=0, end_max=N_MINS_DAY):

        c_orders = self._db_orders['orders']

        cc = c_orders.find({'date': date,
                            'symbol': symbol,
                            'min_start': {'$gte': start_min, '$lte': start_max},
                            'min_end': {'$gte': end_min, '$lte': end_max},
                            },
                           {"_id": 0})
        output = [i for i in cc]
        return output
