__author__ = 'eliazarinelli'

import os
import sys
sys.path.insert(0, os.path.abspath('..'))
from dana.hts import Hts

import time
import numpy as np

N_MINS_DAY = 24*60


def generate_ts(date, start, end, v_minute, volatility):

    price = 100.
    ts = []
    for mins in range(start, end):
        record = {
            'date': date,
            'mins': mins,
            'price': price,
            'volume': v_minute
                 }
        ts.append(record)
        price += volatility * (np.random.rand()-0.5)

    return ts

# #####################################################

if __name__ == '__main__':

    date = 200000
    n_dates = 100
    date_list = list(range(date, date+n_dates))
    symbol_list = ['AAPL', 'MSFT', 'GOOG']
    market_open = 0
    market_close = N_MINS_DAY
    v_minute_market = 100
    volatility = 1.

    hts = Hts(engine_url='mongodb://localhost:27017/', db_name='ts_example')

    try:

        for symbol in symbol_list:

            # drop symbol
            hts.drop_symbol(symbol)

            # insert ts
            for date in date_list:
                ts = generate_ts(date, 0, N_MINS_DAY, v_minute_market, volatility)
                hts.insert_ts(symbol, ts)

            # index on dates
            hts.add_index(symbol)


        t0 = time.time()
        for i in range(2**8):
            hts.get_candle(symbol_list[0], date_list[0], min_end=10)
        print(time.time()-t0)

    except:
        pass

    finally:
        hts.close()