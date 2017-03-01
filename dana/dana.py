import pymongo
from pymongo import MongoClient
import numpy as np
import time

N_MINS_DAY = 24*60
TS_FIELDS = set(['date', 'mins', 'price', 'volume'])
ORDERS_FIELDS = set(['mgr', 'bkr', 'symbol', 'sign', 'date',
                     'min_start', 'min_end', 'volume', 'price', 'ntrades', 'is_ok'])


class OrdersReservoir(object):

    def __init__(self, engine_url, db_name):
        self._client = MongoClient(engine_url)
        self._db_name = db_name
        self._db_orders = self._client[self._db_name]

    def empty_reservoir(self):

        """ Drop the orders database """

        self._client.drop_database(self._db_name)

    def insert_orders(self, orders):

        """ Insert orders in the orders db """

        if set(orders[0].keys()) != ORDERS_FIELDS:
            raise ValueError('Wrong format of input in orders: dict keys should be: ' + ', '.join(ORDERS_FIELDS))

        # get the orders collection
        collection_orders = self._db_orders.get_collection('orders')

        # insert input orders
        collection_orders.insert_many(orders)

    def get_orders(self, symbol=None, date=None):

        """ Get orders corresponding to symbol and date """

        # create filter for symbol and dates
        filter_order = {}
        if date is not None:
            filter_order['date'] = date
        if symbol is not None:
            filter_order['symbol'] = symbol

        # get the collection of orders
        collection_orders = self._db_orders['orders']

        # filtering on the date and symbol
        cc = collection_orders.find(filter_order)
        return list(cc)

    def get_symbols(self, date=None):

        # create filter for date
        filter_order = {}
        if date is not None:
            filter_order['date'] = date

        # get the collection of orders
        collection_orders = self._db_orders['orders']

        symbols = collection_orders.find(filter_order, {'symbol': 1}).distinct('symbol')
        symbols_list = list(symbols)
        symbols_list.sort()
        return symbols_list

    def get_dates(self, symbol=None):

        # create filter symbol
        filter_order = {}
        if symbol is not None:
            filter_order['symbol'] = symbol

        # get the collection of orders
        collection_orders = self._db_orders['orders']

        dates = collection_orders.find(filter_order, {'date': 1}).distinct('date')
        dates_list = list(dates)
        dates_list.sort()
        return dates_list

    def add_market_info(self, order_id, candle_day, candle_period):

        # get the collection of the orders
        collection_orders = self._db_orders['orders']

        try:
            # get the order
            order = collection_orders.find({'_id': order_id})[0]

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


class Hts(object):

    def __init__(self, engine_url, db_name):
        self._client = MongoClient(engine_url)
        self._db_name = db_name
        self._db_hts = self._client[self._db_name]

    def drop_ts(self, symbol=None):

        """
        Drop the ts database

        Input:
        -----
        symbol: string, if provided drop only the associated collection
        """

        if symbol is None:
            self._client.drop_database(self._db_name)
        else:
            self._db_hts.drop_collection(symbol)

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

        collection_symbol = self._db_hts.get_collection(symbol)
        collection_symbol.insert_many(ts)

    def get_ts(self, symbol, date, min_start=0, min_end=N_MINS_DAY):

        """ Get the time series corresponding to a symbol and date """

        # get the collection corresponding to the symbol
        collection_symbol = self._db_hts.get_collection(symbol)

        # get the cursor filtering on the date, min_start and min_end
        # sort by the minutes
        cc = collection_symbol.find(
            {'date': date,
             'mins': {'$gte': min_start, '$lt': min_end}},
            {"_id": 0}).sort([('mins', 1)])

        return list(cc)

    def ts_index(self, symbol):

        # get the collection corresponding to the symbol
        collection_symbol = self._db_hts.get_collection(symbol)
        collection_symbol.create_index([('date', pymongo.ASCENDING)])

    def get_candle(self, symbol, date, min_start=0, min_end=N_MINS_DAY):

        """ Get the candle corresponding to a symbol and date """

        # get the collection corresponding to the symbol
        collection_symbol = self._db_hts.get_collection(symbol)

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
    n_orders = 2**0
    n_dates = 2**0

    reservoir = OrdersReservoir(engine_url='mongodb://localhost:27017/', db_name='orders_example')
    hts = Hts(engine_url='mongodb://localhost:27017/', db_name='ts_example')

    print('Insert ' + str(n_dates) + ' ts:')
    t0 = time.time()
    hts.drop_ts()
    for i in range(date, date+n_dates):
        ts = generate_ts(date=i, start=market_open, end=market_close, v_max=v_max)
        print(len(ts))
        hts.insert_ts(symbol, ts)
    hts.ts_index(symbol=symbol)
    print(time.time() - t0)

    cc = hts.get_candle(symbol=symbol, date=date, min_start=market_open, min_end=market_close)

    print('Insert ' + str(n_orders) + ' orders:')
    t0 = time.time()
    reservoir.empty_reservoir()
    orders_in = []
    orders_in.append(generate_order(symbol=symbol, date=date, min_start=market_open, min_end=market_close))
    orders_in.append(generate_order(symbol=symbol, date=date+1, min_start=market_open, min_end=market_close))
    orders_in.append(generate_order(symbol='MSFT', date=date, min_start=market_open, min_end=market_close))
    orders_in.append(generate_order(symbol='MSFT', date=date+1, min_start=market_open, min_end=market_close))

    reservoir.insert_orders(orders_in)
    print(time.time() - t0)

    order_list = reservoir.get_orders()
    print(len(order_list))
    for order in order_list:
        reservoir.add_market_info(order['_id'], candle_day=cc, candle_period=cc)

    list_dates = reservoir.get_dates(symbol=symbol)
    print(list_dates)

    list_symbols = reservoir.get_symbols()
    print(list_symbols)

