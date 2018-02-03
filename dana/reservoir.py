import pymongo
from pymongo import MongoClient
import numpy as np
import time

ORDERS_FIELDS = set(['mgr', 'bkr', 'symbol', 'sign', 'date',
                     'min_start', 'min_end', 'volume', 'price', 'ntrades', 'duration_ph', 'mi', 'ci'])

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
        filter_in['ci.duration_vol'] = tmp

    if prp_inf is not None or prp_sup is not None:
        tmp = {}
        if prp_inf is not None:
           tmp['$gte'] = prp_inf
        if prp_sup is not None:
           tmp['$lt'] = prp_sup
        filter_in['ci.pr_period'] = tmp

    if prd_inf is not None or prd_sup is not None:
        tmp = {}
        if prd_inf is not None:
           tmp['$gte'] = prd_inf
        if prd_sup is not None:
           tmp['$lt'] = prd_sup
        filter_in['ci.pr_day'] = tmp

    return filter_in


def create_ci(order, candle_day, candle_period, volatility):

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


def create_mi(order, time_series, volatility):

    output = {'available': False}

    try:

        for record in time_series:
            if record['mins']==order['min_start']:
                price_start = record['price']

            output['m_'+str(record['mins']-order['min_start'])] = \
                float(order['sign'])*np.log(float(record['price'])/float(price_start))/volatility

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


    # CREATE #####################################################

    def insert_orders(self, orders):

        """ Receive a list of orders and insert them in the database """

        if set(orders[0].keys()) != ORDERS_FIELDS:
            raise ValueError('Wrong format of input in orders: dict keys should be: ' + ', '.join(ORDERS_FIELDS))

        # get the orders collection
        collection_orders = self._db_orders.get_collection('orders')

        # insert input orders
        collection_orders.insert_many(orders)

    # READ ######################################################

    def get_orders(self, symbol_list=None, mgr_list=None, bkr_list=None, date_list=None, sign=None,
                   start_inf=None, start_sup=None, end_inf=None, end_sup=None,
                   duration_ph_inf=None, duration_ph_sup=None, duration_vol_inf=None, duration_vol_sup=None,
                   prp_inf=None, prp_sup=None, prd_inf=None, prd_sup=None):

        """
        Get orders

        Input:
        ------
        filter values, if provided, the filter applies

        Output:
        -------
        a cursor
        """

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

    def list_symbols(self, date_list=None, mgr_list=None, bkr_list=None):

        """ List of unique symbols, return a list """

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

    def list_dates(self, symbol_list=None, mgr_list=None, bkr_list=None):

        """ List of unique dates, return a list """

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

    # DELETE #############################################################

    def empty_reservoir(self):

        """ Drop the orders database """

        self._client.drop_database(self._db_name)

    # AGGREGATE ##########################################################

    def bucketed_stats(self, field_x, field_y, boundaries, symbol_list=None, mgr_list=None,
                     bkr_list=None, date_list=None, sign=None,
                     start_inf=None, start_sup=None, end_inf=None, end_sup=None,
                     duration_ph_inf=None, duration_ph_sup=None, duration_vol_inf=None, duration_vol_sup=None,
                     prp_inf=None, prp_sup=None, prd_inf=None, prd_sup=None):

        '''

        Calculate the mean and the standard deviation of the x and y
        values of points, bucketing on the x axis

        Input:
        ------
        field_x: string, name of the x axis
        field_y: string, name of the y axis
        boundaries: list, bucket boundaries of the x axis

        Output:
        -------
        cursor
        '''

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

        # filter pipe
        p_match = {'$match': filter_order}

        # bucket pipe
        p_bucket = {'$bucket': {'groupBy': '$'+field_x,
                                'boundaries': boundaries,
                                'default': "Other",
                                'output': {
                                    'm_x': {'$avg': '$'+field_x},
                                    's_x': {'$stdDevPop': '$'+field_x},
                                    'nn': {'$sum': 1},
                                    'm_y': {'$avg': '$'+field_y},
                                    's_y': {'$stdDevPop': '$'+field_y}
                                }}}

        # apply pipeline
        tmp = collection_orders.aggregate([p_match, p_bucket])

        # return cursor
        return tmp

    def average_path(self, duration, symbol_list=None, mgr_list=None,
                     bkr_list=None, date_list=None, sign=None,
                     start_inf=None, start_sup=None, end_inf=None, end_sup=None,
                     prp_inf=None, prp_sup=None, prd_inf=None, prd_sup=None):

        '''
        Average the mi paths for the orders of a given duration

        Input:
        ------
        duration: int, the order duration

        Output:
        -------
        cursor
        '''

        # create filter
        filter_order = _generate_filter(symbol_list=symbol_list, mgr_list=mgr_list, bkr_list=bkr_list,
                                        date_list=date_list, sign=sign,
                                        start_inf=start_inf, start_sup=start_sup,
                                        end_inf=end_inf, end_sup=end_sup,
                                        duration_ph_inf=duration, duration_ph_sup=duration+1,
                                        prp_inf=prp_inf, prp_sup=prp_sup, prd_inf=prd_inf, prd_sup=prd_sup)

        # get the collection of orders
        collection_orders = self._db_orders['orders']

        # generate the filter step
        p_match = {'$match': filter_order}

        # generate the group step
        # average the impact at i-th minute for all the minutes between 0 and the duration selected
        tmp_0 = {'_id': ''}
        for i in range(duration):
            tmp_0[str(i)] = {'$avg': '$mi.m_'+str(i)}
        # count the number of orders
        tmp_0['nn'] = {'$sum': 1}
        # generate the pipe
        p_average = {'$group': tmp_0}

        # aggregate
        tmp = collection_orders.aggregate([p_match, p_average])
        return tmp


