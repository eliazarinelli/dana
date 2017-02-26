import pymongo
from pymongo import MongoClient
import numpy as np
import time

N_MINS_DAY = 24*60
TS_FIELDS = set(['date', 'mins', 'price', 'volume'])
ORDERS_FIELDS = set(['mgr', 'bkr', 'symbol', 'sign', 'date',
                     'min_start', 'min_end', 'volume', 'price', 'ntrades', 'is_ok'])


class Dana(object):

    def __init__(self, engine_url, db_ts_name, db_orders_name):

        self._db_ts_name = db_ts_name
        self._db_orders_name = db_orders_name
        self._client = MongoClient(engine_url)
        self._db_ts = self._client[db_ts_name]
        self._db_orders = self._client[db_orders_name]

    def drop_ts(self, symbol=None):

        """
        Drop the ts database

        Input:
        -----
        symbol: string, if provided drop only the associated collection
        """

        if symbol is None:
            self._client.drop_database(self._db_ts_name)
        else:
            self._db_ts.drop_collection(symbol)

    def insert_ts(self, symbol, ts):

        """
        Insert a time series in the symbol collection

        Input:
        ------
        symbol: string, the collection
        ts: dict, the time series

        """

        if set(ts[0].keys()) != TS_FIELDS:
            raise ValueError('Wrong format of input in ts: dict keys should be: ' + ', '.join(TS_FIELDS))

        collection_symbol = self._db_ts.get_collection(symbol)
        collection_symbol.insert_many(ts)

    def get_ts(self, symbol, date, min_start=0, min_end=N_MINS_DAY):

        """ Get the time series corresponding to a symbol and date """

        # get the collection corresponding to the symbol
        collection_symbol = self._db_ts.get_collection(symbol)

        # get the cursor filtering on the date, min_start and min_end
        # sort by the minutes
        cc = collection_symbol.find(
            {'date': date,
             'mins': {'$gte': min_start, '$lt': min_end}},
            {"_id": 0}).sort([('mins', 1)])

        return list(cc)

    def ts_index(self, symbol):

        # get the collection corresponding to the symbol
        collection_symbol = self._db_ts.get_collection(symbol)
        collection_symbol.create_index([('date', pymongo.ASCENDING)])

    def get_candle(self, symbol, date, min_start=0, min_end=N_MINS_DAY):

        """ Get the candle corresponding to a symbol and date """

        # get the collection corresponding to the symbol
        collection_symbol = self._db_ts.get_collection(symbol)

        # get the cursor filtering on the date, min_start and min_end
        # sort by the minutes
        pipeline = [
            {'$match': {'date': date, 'mins': {'$gte': min_start, '$lt': min_end}}},
            {'$sort': {'mins': 1}},
            {'$project': {'price': 1, 'volume': 1, 'pv': {'$multiply': ['$price', '$volume']}}},
            {'$group': {
                '_id': '',
                'open': {'$first': '$price'},
                'high': {'$max': '$price'},
                'low': {'$min': '$price'},
                'close': {'$last': '$price'},
                'volume': {'$sum': '$volume'},
                'pv': {'$sum': '$pv'}
                }}
        ]

        cc = list(collection_symbol.aggregate(pipeline))

        if len(cc) == 0:
            return {}
        else:
            candle = cc[0]
            candle['vwap'] = float(candle['pv'])/float(candle['volume'])
            del candle['_id']
            del candle['pv']
            return candle

    def drop_orders(self):

        """ Drop the orders database """

        self._client.drop_database(self._db_orders_name)

    def insert_orders(self, orders):

        """ Insert orders in the orders db """

        if set(orders_in[0].keys()) != ORDERS_FIELDS:
            raise ValueError('Wrong format of input in orders: dict keys should be: ' + ', '.join(ORDERS_FIELDS))

        # get the orders collection
        collection_orders = self._db_orders.get_collection('orders')

        # insert input orders
        collection_orders.insert_many(orders)

    def get_orders(self, symbol, date, start_min=0, start_max=N_MINS_DAY, end_min=0, end_max=N_MINS_DAY):

        """ Get orders corresponding to symbol and date """

        # get the collection of orders
        collection_orders = self._db_orders['orders']

        # filtering on the date and symbol
        # filtering on min_start and min_end
        # do not return id
        cc = collection_orders.find({'date': date,
                                     'symbol': symbol,
                                     'min_start': {'$gte': start_min, '$lte': start_max},
                                     'min_end': {'$gte': end_min, '$lte': end_max},
                                     },
                                    {"_id": 0})
        return list(cc)

    def assign_candle_orders(self, symbol, date):

        """ Assign and store in the bd for each order corresponding
        to symbol and date the day and period candle """


        # get the collection of the orders
        collection_orders = self._db_orders['orders']

        # get the cursor of the order corresponding to symbol and date
        cursor_orders = collection_orders.find(
            {'date': date,
             'symbol': symbol})

        # get the day_candle
        candle_day = self.get_candle(symbol=symbol, date=date)

        for order in cursor_orders:

            if not order['is_ok']:
                try:
                    # get the candle period
                    candle_period = self.get_candle(symbol, date, min_start=order['min_start'], min_end=order['min_end'])

                    # participation rate day
                    pr_day = float(order['volume'])/float(candle_day['volume'])

                    # participation rate period
                    pr_period = float(order['volume'])/float(candle_period['volume'])

                    # duration volume time
                    duration_vol = float(candle_period['volume'])/float(candle_day['volume'])

                    # duration physical time
                    duration_ph = order['min_end'] - order['min_start']

                    # daily volatility
                    volatility_day = float(candle_day['high']-candle_day['low'])/float(candle_day['open'])

                    # temporary impact
                    impact_end = order['sign'] * np.log(float(candle_period['close'])/candle_period['open'])/volatility_day

                    # impact vwap
                    impact_vwap = order['sign'] * np.log(float(order['price'])/candle_period['open'])/volatility_day

                    collection_orders.update_one(
                        {'_id': order['_id']},
                        {'$set': {
                            'is_ok': True,
                            'candle_period': candle_period,
                            'candle_day': candle_day,
                            'pr_day': pr_day,
                            'pr_period': pr_period,
                            'duration_vol': duration_vol,
                            'duration_ph': duration_ph,
                            'volatility_day': volatility_day,
                            'impact_end': impact_end,
                            'impact_vwap': impact_vwap,
                        }})
                except:
                    pass

    def get_available_symbol(self):

        # get the collection of orders
        collection_orders = self._db_orders['orders']

        symbols = collection_orders.find({}, {'symbol': 1}).distinct('symbol')
        symbols_list = list(symbols)
        symbols_list.sort()
        return symbols_list

    def get_available_dates(self):

        # get the collection of orders
        collection_orders = self._db_orders['orders']

        dates = collection_orders.find({}, {'date': 1}).distinct('date')
        dates_list = list(dates)
        dates_list.sort()
        return dates_list

# ##############################################################################


def generate_ts(date, start, end, v_max):

    price = 100.
    ts = []
    for mins in range(start, end):
        record = {
            'date': date,
            'mins': mins,
            'price': price,
            'volume': v_max
                 }
        ts.append(record)
        price += 1

    return ts


def generate_order(symbol, date, min_start, min_end):

    """ Generate a random order """

    # maximum volume of an order
    v_max = 10000

    # maximum number of trades per order
    n_max = 10

    # random start and end time
    t_random = [0, 0]
    while t_random[0] == t_random[1]:
        t_random = np.random.randint(min_start, min_end, 2)

    order = {
        'mgr': 'mgr_test',
        'bkr': 'bkr_test',
        'symbol': symbol,
        'sign': 1 if np.random.rand() < 0.5 else -1,
        'date': date,
        'min_start': int(str(min(t_random))),
        'min_end': int(str(max(t_random))),
        'volume': np.random.randint(0, v_max),
        'price': 100. + 10.*np.random.rand(),
        'ntrades': np.random.randint(0, n_max),
        'is_ok': False
    }

    return order

if __name__ == '__main__':

    date = 200000
    symbol = 'AAPL'
    market_open = 1
    market_close = 2**10
    v_max = 1
    n_orders = 2**8
    n_dates = 2**6

#    api = Dana(engine_url='mongodb://192.168.1.12:27017/', db_ts_name='ts_example', db_orders_name='orders_example')
    api = Dana(engine_url='mongodb://localhost:27017/', db_ts_name='ts_example', db_orders_name='orders_example')

    print('drop db')
    t0 = time.time()
    api.drop_ts()
    api.drop_orders()
    print(time.time() - t0)

    print('Insert ' + str(n_dates) + ' ts:')
    t0 = time.time()
    for i in range(date, date+n_dates):
        ts = generate_ts(date=i, start=market_open, end=market_close, v_max=v_max)
        api.insert_ts(symbol, ts)
    api.ts_index(symbol=symbol)
    print(time.time() - t0)

    print('Insert ' + str(n_orders) + ' orders:')
    t0 = time.time()
    orders_in = [generate_order(symbol=symbol, date=date, min_start=market_open, min_end=market_close) for i in range(n_orders)]
    api.insert_orders(orders_in)
    print(time.time() - t0)

    print('Insert candles:')
    t0 = time.time()
    api.assign_candle_orders(symbol=symbol, date=date)
    print(time.time() - t0)

