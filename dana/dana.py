import pymongo
from pymongo import MongoClient
import numpy as np
import time

N_MINS_DAY = 24*60
TS_FIELDS = set(['date', 'mins', 'price', 'volume'])
ORDERS_FIELDS = set(['mgr', 'bkr', 'symbol', 'sign', 'date',
                     'min_start', 'min_end', 'volume', 'price', 'ntrades', 'duration_ph', 'mi'])


def _generate_filter(symbol_list=None, mgr_list=None, bkr_list=None, date_list=None, sign=None,
                     start_inf=None, start_sup=None, end_inf=None, end_sup=None,
                     duration_ph_inf=None, duration_ph_sup=None, duration_vol_inf=None, duration_vol_sup=None,
                     prp_inf=None, prp_sup=None, prd_inf=None, prd_sup=None):

    # empty output
    filter_in = {}

    if symbol_list is not None:
        filter_in['symbol'] = {'$in': symbol_list}

    if date_list is not None:
        filter_in['date'] = {'$in': date_list}

    if mgr_list is not None:
        filter_in['mgr'] = {'$in': mgr_list}

    if bkr_list is not None:
        filter_in['bkr'] = {'$in': bkr_list}

    if sign is not None:
        filter_in['sign'] = sign

    if start_inf is not None or start_sup is not None:
        tmp = {}
        if start_inf is not None:
           tmp['$gte'] = start_inf
        if start_sup is not None:
           tmp['$lt'] = start_sup
        filter_in['min_start'] = tmp

    if end_inf is not None or end_sup is not None:
        tmp = {}
        if end_inf is not None:
           tmp['$gte'] = end_inf
        if start_sup is not None:
           tmp['$lt'] = end_sup
        filter_in['min_end'] = tmp

    if duration_ph_inf is not None or duration_ph_sup is not None:
        tmp = {}
        if duration_ph_inf is not None:
           tmp['$gte'] = duration_ph_inf
        if duration_ph_sup is not None:
           tmp['$lt'] = duration_ph_sup
        filter_in['duration_ph'] = tmp

    if duration_vol_inf is not None or duration_vol_sup is not None:
        tmp = {}
        if duration_vol_inf is not None:
           tmp['$gte'] = duration_vol_inf
        if duration_ph_sup is not None:
           tmp['$lt'] = duration_vol_sup
        filter_in['mi.duration_vol'] = tmp

    if prp_inf is not None or prp_sup is not None:
        tmp = {}
        if prp_inf is not None:
           tmp['$gte'] = prp_inf
        if prp_sup is not None:
           tmp['$lt'] = prp_sup
        filter_in['mi.pr_period'] = tmp

    if prd_inf is not None or prd_sup is not None:
        tmp = {}
        if prd_inf is not None:
           tmp['$gte'] = prd_inf
        if prd_sup is not None:
           tmp['$lt'] = prd_sup
        filter_in['mi.pr_day'] = tmp

    return filter_in


def create_mi(order, candle_day, candle_period, volatility):

    output = {'available': False}

    try:
        # participation rate day
        output['pr_day'] = float(order['volume'])/float(candle_day['volume'])

        # participation rate period
        output['pr_period'] = float(order['volume'])/float(candle_period['volume'])

        # duration volume time
        output['duration_vol'] = float(candle_period['volume'])/float(candle_day['volume'])

        # impact end to start
        output['impact_co'] = float(order['sign'])*np.log(float(candle_period['close'])/float(candle_period['open']))/volatility

        # impact vwap versus start
        output['impact_vo'] = float(order['sign'])*np.log(float(order['price'])/float(candle_period['open']))/volatility

        # impact vwap versus start
        output['impact_vv'] = float(order['sign'])*np.log(float(order['price'])/float(candle_period['vwap']))/volatility

        # availability
        output['available'] = True

    except:
        pass

    return output



class OrdersReservoir(object):

    def __init__(self, engine_url, db_name):
        self._client = MongoClient(engine_url)
        self._db_name = db_name
        self._db_orders = self._client[self._db_name]

    def close(self):
        self._client.close()

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

    def add_market_info(self, order_id, candle_day, candle_period):

        """ Add the market info to an order: duration, period and day participation rate """

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

            # update value
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
                }})
        except:
            pass

    def add_impact(self, order_id, impact_value):

        """ Add the impact to an order """

        # get the collection of the orders
        collection_orders = self._db_orders['orders']

        # update value
        collection_orders.update_one(
            {'_id': order_id},
            {'$set': {'impact': impact_value}})

    def get_orders(self, symbol_list=None, mgr_list=None, bkr_list=None, date_list=None, sign=None,
                   start_inf=None, start_sup=None, end_inf=None, end_sup=None,
                   duration_ph_inf=None, duration_ph_sup=None, duration_vol_inf=None, duration_vol_sup=None,
                   prp_inf=None, prp_sup=None, prd_inf=None, prd_sup=None):

        """ Get orders """

        # create filter
        filter_order = _generate_filter(symbol_list=symbol_list, mgr_list=mgr_list, bkr_list=bkr_list,
                                        date_list=date_list, sign=sign,
                                        start_inf=start_inf, start_sup=start_sup,
                                        end_inf=end_inf, end_sup=end_sup,
                                        duration_ph_inf=duration_ph_inf, duration_ph_sup=duration_ph_sup,
                                        duration_vol_inf=duration_vol_inf, duration_vol_sup=duration_vol_sup,
                                        prp_inf=prp_inf, prp_sup=prp_sup, prd_inf=prd_inf, prd_sup=prd_sup)

        # get the collection of orders
        collection_orders = self._db_orders['orders']

        # apply filter
        orders_cursor = collection_orders.find(filter_order)

        return orders_cursor

    def get_symbols(self, date_list=None, mgr_list=None, bkr_list=None):

        """ Get list of unique symbols """

        # create filter
        filter_order = _generate_filter(date_list=date_list, mgr_list=mgr_list, bkr_list=bkr_list)

        # get the collection of orders
        collection_orders = self._db_orders['orders']

        # get distinct values of symbol using filter
        symbols = collection_orders.find(filter_order, {'symbol': 1}).distinct('symbol')

        # transform to list and sort
        symbols_list = list(symbols)
        symbols_list.sort()

        return symbols_list

    def get_dates(self, symbol_list=None, mgr_list=None, bkr_list=None):

        """ Get list of unique dates """

        # create filter
        filter_order = _generate_filter(symbol_list=symbol_list, mgr_list=mgr_list, bkr_list=bkr_list)

        # get the collection of orders
        collection_orders = self._db_orders['orders']

        # get distinct values of dates applying filter
        dates = collection_orders.find(filter_order, {'date': 1}).distinct('date')

        # transform to list and sort
        dates_list = list(dates)
        dates_list.sort()

        return dates_list

    def bucket_stuff(self, field_x, field_y, boundaries, symbol_list=None, mgr_list=None,
                     bkr_list=None, date_list=None, sign=None,
                     start_inf=None, start_sup=None, end_inf=None, end_sup=None,
                     duration_ph_inf=None, duration_ph_sup=None, duration_vol_inf=None, duration_vol_sup=None,
                     prp_inf=None, prp_sup=None, prd_inf=None, prd_sup=None):

        # create filter
        filter_order = _generate_filter(symbol_list=symbol_list, mgr_list=mgr_list, bkr_list=bkr_list,
                                        date_list=date_list, sign=sign,
                                        start_inf=start_inf, start_sup=start_sup,
                                        end_inf=end_inf, end_sup=end_sup,
                                        duration_ph_inf=duration_ph_inf, duration_ph_sup=duration_ph_sup,
                                        duration_vol_inf=duration_vol_inf, duration_vol_sup=duration_vol_sup,
                                        prp_inf=prp_inf, prp_sup=prp_sup, prd_inf=prd_inf, prd_sup=prd_sup)

        # get the collection of orders
        collection_orders = self._db_orders['orders']

        p_match = {'$match': filter_order}

        p_bucket = {'$bucket': {'groupBy': '$'+field_x,
                                'boundaries': boundaries,
                                'default': "Other",
                                'output': {
                                    'm_x': {'$avg': '$'+field_x},
                                    's_x': {'$stdDevPop': '$'+field_x},
                                    'm_y': {'$avg': '$'+field_y},
                                    's_y': {'$stdDevPop': '$'+field_y}
                                }}}

        tmp = collection_orders.aggregate([p_match, p_bucket])
        return tmp


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

