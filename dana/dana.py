from .userconfig import *

from .models import Orders, DayInfo, PeriodInfo

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


class Hts(object):

    def __init__(self, engine_url, database_name):

        self.client = MongoClient(engine_url)
        self.db = self.client[database_name]

    def get_ts(self, symbol, date, start=0, end=N_MINS_DAY):

        c_symbol = self.db[symbol]

        cc = c_symbol.find({'date': date, 'mins': {'$gte': start, '$lt': end}})

        output = {}
        for record in cc:
            output[record['mins']] = {
                'price': record['price'],
                'volume': record['volume']
            }
        return output

    def get_candle(self, symbol, date, start=0, end=N_MINS_DAY):

        ts = self.get_ts(symbol, date, start, end)

        output = {
            'open': ts[min(ts.keys())]['price'],
            'high': max([i['price'] for i in ts.values()]),
            'low': min([i['price'] for i in ts.values()]),
            'close': ts[max(ts.keys())]['price'],
            'volume': sum([i['volume'] for i in ts.values()]),
            'vwap': sum([i['volume']*i['price'] for i in ts.values()])/sum([i['volume'] for i in ts.values()])
        }

        return output