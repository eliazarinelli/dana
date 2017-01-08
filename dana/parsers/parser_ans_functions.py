__author__ = 'eliazarinelli'

import datetime
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import IntegrityError

DBSession = scoped_session(sessionmaker())
engine = None

def _trade_extract_dict(dict_in, name_mapping):

    """
    Select the elements of the input dictionary
    and stores in the output with a new key.
    The keys of name_mapping are the new key names,
    the values are the old key names
    """

    dict_out = {}

    for k_out, k_in in name_mapping.items():
        try:
            dict_out[k_out] = dict_in[k_in]
        except KeyError:
            raise KeyError('Missing key ' + str(k_out))

    return dict_out


def _trade_cast_int(trade_in, fields_int):

    """ Cast integer fields	"""

    for k_field in fields_int:
        trade_in[k_field] = int(trade_in[k_field])


def _trade_cast_float(trade_in, fields_float):

    """ Cast float fields	"""

    for k_field in fields_float:
        trade_in[k_field] = float(trade_in[k_field])


def _trade_extract_julian_date(trade_in, field_date_in, field_date_out):

    """ Extract the julian date from a date in format yyyy-mm-dd hh:mm:ss """

    # datetime format of the placement time
    dt_trade = datetime.datetime.strptime(trade_in[field_date_in], '%Y-%m-%d %H:%M:%S')

    # julian date of the placement time
    trade[field_date_out] = dt_trade.toordinal()


def _trade_extract_min(trade_in, field_min_in, field_min_out):

    """ Extract the minute from midnight from a date in format yyyy-mm-dd hh:mm:ss """

    # extract datetime format
    dt_trade = datetime.datetime.strptime(trade_in[field_min_in], '%Y-%m-%d %H:%M:%S')

    # minute from midnight
    trade[field_min_out] = dt_trade.hour*60 + dt_trade.minute


def _trade_add_vp(trade_in, field_volume, field_price, field_vp):

    """ Calculate the field trade_volume*trade_price """

    trade[field_vp] = float(trade[field_volume]) * trade[field_price]


def _trade_adjust_client(trade_in, field_client, field_add_in, field_add_out):

    """ Join client code with the mgr or bkr code """

    trade_in[field_add_out] = '_'.join([trade_in[field_client], trade_in[field_add_in]])


def _order_calculate_vwap(order_in, field_volume_in, field_price_in, field_volume_out, field_price_out):

    """ Calculate the price of an order and remove the dict the input fields """

    # report the volume
    order_in[field_volume_out] = order_in[field_volume_in]

    # calculate the price
    order_in[field_price_out] = order_in[field_price_in] / float(order_in[field_volume_in])

    # remove the old volume
    del order_in[field_volume_in]

    # remove the old price
    del order_in[field_price_in]


def _extract_orders(trade_list_in, fields_key_gby, fields_sum_gby, field_count):
    """
    Group the trades with same key and generate the orders

    Group-by the trades with same key, each group correspond to an order.
    Sum the volume and the volume*price fields of each group.
    Count the number of records in each group (trades in an order).

    :param trade_list_in: list, each entry is dict and correspond to a trade
    :param fields_key_gby: list, field of the key
    :param fields_sum_gby: list, field of the sum
    :param field_count: str, name of the counting field
    :return: list, each entry is a dict and correspond to an order
    """
    # create dataframe
    df = pd.DataFrame(trade_list_in)

    # FIRST AGGREGATION
    df_gby_1 = df.groupby(fields_key_gby)

    # sum volume and price*volume
    df_tmp_1 = df_gby_1[fields_sum_gby].sum()

    # count the number of trades
    df_tmp_2 = df_gby_1[fields_sum_gby].count()
    hh = [field_count] * len(fields_sum_gby)
    df_tmp_2.columns = hh
    df_tmp_2 = df_tmp_2[hh[0]]

    # concatenate the output
    df_tmp_3 = pd.concat([df_tmp_1, df_tmp_2], axis=1)

    # resetting the index of the grouped dataframe
    # this operation transforms the index of a dataframe into a field
    df_grouped_1 = df_tmp_3.reset_index()

    return df_grouped_1.to_dict(orient='records')


def _order_remove_duplicates(order_list, fields_hash):

    """ Remove the duplicates of an order by checking the hash of the key """

    order_dict = {}

    for order_in in order_list:
        hh = hash('_'.join([str(order_in[k]) for k in fields_hash]))

        # put the order in the dict with as key the hash of the key fields
        # if a key is already in the dict the entry is over-written
        # this ensures only one order per key
        order_dict[hh] = order_in

    return list(order_dict.values())


def _order_consistency_check(order_list, field_volume, field_volume_inf,
                             field_price, field_price_inf, name_dict, threshold):

    """
    Check if the price and volume of the inferred order are consistent
    with the one reported.
    """

    order_list_out = []
    for order in order_list:
        if order[field_volume] == order[field_volume_inf] and \
            abs(order[field_price] - order[field_price_inf])/order[field_price] < threshold:

            new_order = {k: order[v] for k, v in name_dict.items()}
            order_list_out.append(new_order)

    return order_list_out


def _init_sqlalchemy():

    global engine
    engine = create_engine(ENGINE_URL, echo=False)
    DBSession.remove()
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)


if __name__ == '__main__':

    import gzip
    import csv
    import time

    import os
    import sys
    sys.path.insert(0, os.path.abspath('../../'))

    from dana.parsers.conf_ans import *
    from dana.models import Base, Orders
    from dana.userconfig import ENGINE_URL

    t_0 = time.time()

    print('Reading trades...')

    trade_list = []

    with gzip.open(FILE_RAW, 'rt') as file_gz:

        # reading the input file line by line
        reader = csv.DictReader(file_gz, delimiter='|')

        for row in reader:
            trade = _trade_extract_dict(row, NAME_MAPPING_ANS)

            if not all(trade.values()):
                print('Error in input values: skipping trade')
                continue

            try:
                _trade_cast_int(trade, FIELD_int)
            except:
                print('Error in casting int fields: skipping trade')
                continue

            try:
                _trade_cast_float(trade, FIELD_float)
            except:
                print('Error in casting float fields: skipping trade')
                continue

            try:
                _trade_extract_julian_date(trade, FIELD_datetime_start, FIELD_date_julian)
            except:
                print('Error extracting julian date: skipping trade')
                continue

            try:
                _trade_extract_min(trade, FIELD_datetime_start, FIELD_time_start)
            except:
                print('Error extracting minute start: skipping trade')
                continue

            try:
                _trade_extract_min(trade, FIELD_datetime_end, FIELD_time_end)
            except:
                print('Error extracting minute start: skipping trade')
                continue

            try:
                _trade_add_vp(trade, FIELD_trade_price, FIELD_trade_volume, FIELD_trade_vp)
            except:
                print('Error calculating volume*price: skipping trade')
                continue

            try:
                _trade_adjust_client(trade, FIELD_clientcode, FIELD_clientmgrcode, FIELD_mgr)
            except:
                print('Error adjusting client code')
                continue

            try:
                _trade_adjust_client(trade, FIELD_clientcode, FIELD_clientbkrcode, FIELD_bkr)
            except:
                print('Error adjusting client code')
                continue


            trade_out = {k: trade[k] for k in FIELDS_to_keep}

            trade_list.append(trade_out)

    print('done in secs: ' + str(time.time() - t_0))

    print('Extracting orders...')

    t_0 = time.time()

    order_list = _extract_orders(trade_list, FIELDS_KEY_GBY, FIELDS_SUM_GBY, FIELD_count)

    for order in order_list:
        _order_calculate_vwap(order, FIELD_trade_volume, FIELD_trade_vp,
                              FIELD_order_volume_inf, FIELD_order_price_inf)

    order_list = _order_remove_duplicates(order_list, FIELDS_KEY_GBY)

    order_list_check = _order_consistency_check(order_list, FIELD_order_volume, FIELD_order_volume_inf,
                                          FIELD_order_price, FIELD_order_price_inf,
                                          ORDERS_NAME_MAP, THRESHOLD_VWAP)

    print('done in secs: ' + str(time.time() - t_0))

    print('Extracting orders...')

    t_0 = time.time()

    _init_sqlalchemy()

    try:
        engine.execute(Orders.__table__.insert(), order_list_check)
        print(
            "SQLAlchemy Core: Total time for " + str(len(order_list_check)) +
            " records " + str(time.time() - t_0) + " secs")

    except IntegrityError:
        print('At least one of the records that you are trying \n'
              'to insert is already present in the database. \n'
              'No new record written.')