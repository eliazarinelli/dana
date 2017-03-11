
import os
import sys
sys.path.insert(0, os.path.abspath('..'))
from dana.reservoir import OrdersReservoir, create_mi

import time
import numpy as np


def create_order(symbol, date, min_start, min_end):

    """ Generate a random order """

    # maximum number of trades per order
    n_max = 10

    # volume per minute
    v_minute = 10

    # price base
    price_base = 100.

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
        'volume': v_minute * (int(str(max(t_random)))-int(str(min(t_random)))),
        'price': price_base + np.random.rand(),
        'ntrades': np.random.randint(0, n_max),
        'duration_ph': int(str(max(t_random)))- int(str(min(t_random))),
        'mi': {'available': False}
    }

    return order

# #####################################################

if __name__ == '__main__':

    date = 200000
    symbol = 'AAPL'
    market_open = 0
    market_close = 100
    v_max = 1
    n_orders = 2**5

    v_minute_market = 100

    reservoir = OrdersReservoir(engine_url='mongodb://localhost:27017/', db_name='orders_example')
    #reservoir = OrdersReservoir(engine_url='mongodb://192.168.1.12:27017/', db_name='orders_example')


    try:
        print('Empty reservoir:')
        t0 = time.time()
        reservoir.empty_reservoir()
        print(time.time() - t0)

        orders_list = [create_order(symbol=symbol, date=date, min_start=market_open, min_end=market_close) for i in range(n_orders)]

        candle_day = {
                'open': 100.,
                'close': 110.,
                'high': 120.,
                'low': 90.,
                'volume': v_minute_market * (market_close-market_open),
                'vwap': 100.
            }

        for order in orders_list:

            candle_period = {
                    'open': 100.,
                    'close': 110.,
                    'high': 120.,
                    'low': 90.,
                    'volume': v_minute_market * (order['min_end'] - order['min_start']),
                    'vwap': 100.
                }

            mi = create_mi(order, candle_day, candle_period, volatility=1.)
            order['mi'] = mi

        print('Insert ' + str(n_orders) + ' orders:')
        t0 = time.time()
        reservoir.insert_orders(orders_list)
        print(time.time() - t0)

        print('Bucketing ' + str(n_orders) + ' orders:')
        t0 = time.time()
        tmp = list(reservoir.bucket_stuff(field_x='min_start', field_y='min_start', boundaries=[0, 10, 20, 1000]))
        print(time.time() - t0)

    except:
        pass

    finally:
        reservoir.close()

