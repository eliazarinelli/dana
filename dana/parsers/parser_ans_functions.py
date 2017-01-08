__author__ = 'eliazarinelli'

import datetime

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

    # datetime format of the placement time
    dt_trade = datetime.datetime.strptime(trade_in[field_date_in], '%Y-%m-%d %H:%M:%S')

    # julian date of the placement time
    trade[field_date_out] = dt_trade.toordinal()


def _trade_extract_min(trade_in, field_min_in, field_min_out):

    # extract datetime format
    dt_trade = datetime.datetime.strptime(trade_in[field_min_in], '%Y-%m-%d %H:%M:%S')

    # minute from midnight
    trade[field_min_out] = dt_trade.hour*60 + dt_trade.minute


if __name__ == '__main__':

    import gzip
    import csv

    # Name of the input file
    FILE_RAW = '/Users/eliazarinelli/db/raw/ciao.txt.gz'

    FIELD_ans_clientcode = 'clientcode'
    FIELD_ans_clientmgrcode = 'clientmgrcode'
    FIELD_ans_clientbkrcode = 'clientbkrcode'
    FIELD_ans_symbol = 'symbol'
    FIELD_ans_side = 'side'
    FIELD_ans_trade_volume = 'volume'
    FIELD_ans_trade_price = 'Price'
    FIELD_ans_order_volume = 'xv'
    FIELD_ans_order_price = 'xp'
    FIELD_ans_datetime_start = 'xdtP'
    FIELD_ans_datetime_end = 'xdtX'

    FIELD_clientcode = 'client'
    FIELD_clientmgrcode = 'mgr'
    FIELD_clientbkrcode = 'bkr'
    FIELD_symbol = 'symbol'
    FIELD_side = 'side'
    FIELD_trade_volume = 'trade_volume'
    FIELD_trade_price = 'trade_price'
    FIELD_order_volume = 'order_volume'
    FIELD_order_price = 'order_price'
    FIELD_datetime_start = 'datetime_start'
    FIELD_datetime_end = 'datetime_end'

    NAME_MAPPING_ANS = {
        FIELD_clientcode: FIELD_ans_clientcode,
        FIELD_clientmgrcode: FIELD_ans_clientmgrcode,
        FIELD_clientbkrcode: FIELD_ans_clientbkrcode,
        FIELD_symbol: FIELD_ans_symbol,
        FIELD_side: FIELD_ans_side,
        FIELD_trade_volume: FIELD_ans_trade_volume,
        FIELD_trade_price: FIELD_ans_trade_price,
        FIELD_order_volume: FIELD_ans_order_volume,
        FIELD_order_price: FIELD_ans_order_price,
        FIELD_datetime_start: FIELD_ans_datetime_start,
        FIELD_datetime_end: FIELD_ans_datetime_end
    }

    FIELD_int = [FIELD_side, FIELD_trade_volume, FIELD_order_volume]
    FIELD_float = [FIELD_trade_price, FIELD_order_price]

    FIELD_date_julian = 'date'
    FIELD_time_start = 'time_start'
    FIELD_time_end = 'time_end'

    list_output = []

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