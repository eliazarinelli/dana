
import os
import sys
sys.path.insert(0, os.path.abspath('..'))
from dana.reservoir import OrdersReservoir, create_mi, create_ci

from dana.hts import Hts

import time
import numpy as np


def random_times(min_start, min_end):

    # random start and end time
    t_random = [0, 0]
    while t_random[0] == t_random[1]:
        t_random = np.random.randint(min_start, min_end, 2)

    t_start = int(str(min(t_random)))
    t_end = int(str(max(t_random)))
    return t_start, t_end


def create_order(symbol, date, min_start, min_end, v_minute):

    """ Generate a random order """

    # maximum number of trades per order
    n_max = 10

    # price base
    price_base = 100.

    order = {
        'mgr': 'mgr_test',
        'bkr': 'bkr_test',
        'symbol': symbol,
        'sign': 1 if np.random.rand() < 0.5 else -1,
        'date': date,
        'min_start': min_start,
        'min_end': min_end,
        'volume': v_minute * (min_end-min_start),
        'price': price_base + np.random.rand(),
        'ntrades': np.random.randint(0, n_max),
        'duration_ph': min_end-min_start,
        'mi': {'available': False}
    }

    return order

# #####################################################

if __name__ == '__main__':

    N_MINS_DAY = 24*60

    date = 200000
    n_dates_orders = 100
    date_list = list(range(date, date+n_dates_orders))
    symbol_list = ['AAPL', 'MSFT', 'GOOG']
    market_open = 0
    market_close = N_MINS_DAY
    n_orders_day = 20
    v_minute_order = 10.

    reservoir = OrdersReservoir(engine_url='mongodb://localhost:27017/', db_name='orders_example')
    #reservoir = OrdersReservoir(engine_url='mongodb://192.168.1.12:27017/', db_name='orders_example')

    hts = Hts(engine_url='mongodb://localhost:27017/', db_name='ts_example')


    try:

        if False:
            print('Empty reservoir:')
            t0 = time.time()
            reservoir.empty_reservoir()
            print(time.time() - t0)

            for symbol in symbol_list:
                for date in date_list:

                    print(symbol, date)
                    order_list = []
                    for i_order in range(n_orders_day):
                        min_start, min_end = random_times(market_open, market_close)
                        order_list.append(create_order(symbol=symbol, date=date, min_start=min_start, min_end=min_end, v_minute=v_minute_order))

                    candle_day = hts.get_candle(symbol, date, market_open, market_close)

                    for order in order_list:

                        candle_period = hts.get_candle(symbol, date, order['min_start'], order['min_end'])
                        ci = create_ci(order, candle_day, candle_period, volatility=1.)
                        order['ci'] = ci

                        time_series = hts.get_ts(symbol, date, order['min_start'], order['min_end'])
                        mi = create_mi(order, time_series, volatility=1.)
                        order['mi'] = mi

                    reservoir.insert_orders(order_list)

        t0 = time.time()
        tmp = reservoir.bucket_stuff_2(100)
        a = list(tmp)
        for i in a[0].items():
            print(i)
        print(time.time()-t0)

        t0 = time.time()
        tmp = reservoir.bucketed_stats('ci.duration_vol', 'volume', list(np.linspace(0., 1., 10)))
        a = list(tmp)
        print(a)
        print(time.time()-t0)

    except:
        pass

    finally:
        reservoir.close()

