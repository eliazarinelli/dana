__author__ = 'eliazarinelli'

import os
import numpy as np
import pickle


def _generate_orders(n_orders=2):

    """ Generate a list of random orders """

    # initialisation of the output
    order_list = []

    # available symbols
    symbol_list = ['AAPL', 'MSFT', 'GOOG']

    # available dates
    date_list = list(range(735599, 735599+5))

    # open and close of the market (minutes from midnight)
    t_min = 570
    t_max = 1050

    # maximum volume of an order
    v_max = 10000

    n_max = 10

    for i in range(n_orders):

        t_start = np.random.randint(t_min, t_max)
        t_end = np.random.randint(t_start, t_max)

        order = {
            'mgr': 'mgr_test',
            'bkr': 'bkr_test',
            'symbol': symbol_list[np.random.randint(0, len(symbol_list))],
            'side': 1 if np.random.rand() < 0.5 else -1,
            'date_exec': date_list[np.random.randint(0, len(date_list))],
            'time_start': t_start,
            'time_end': t_end,
            'volume': np.random.randint(0, v_max),
            'price': 100. + 10.*np.random.rand(),
            'n_trades': np.random.randint(0, n_max)
        }

        order_list.append(order)

    return order_list


def _store_orders(order_list, path_stage):

    """ Store the orders into a pickle file """

    # create the folder where to store the file
    if not os.path.exists(path_stage):
        os.makedirs(path_stage)

    # dump the file
    pickle.dump(order_list, open('/'.join([path_stage, 'example.p']), "wb"))


if __name__ == '__main__':

    # create the orders
    n_orders = 2**10
    list_orders = _generate_orders(n_orders)

    # store the orders
    path_stage = os.path.relpath('../../db/stage')
    _store_orders(list_orders, path_stage)

    print('Generated ' + str(n_orders) + ' orders and stored in:')
    print('/'.join([path_stage, 'example.p']))

