import numpy as np 
import pandas as pd
import datetime
import gzip
import csv

from configurations import *

def _import_dict(path_input):

    list_output = []

    with gzip.open(path_input, 'rt') as file_gz:

        # reading the input file line by line
        reader = csv.DictReader(file_gz, delimiter='|')

        for row in reader:
            if all([row[k] for k in FIELDS_INT + FIELDS_STR + FIELDS_FLOAT]):
                dict_int = {k: int(row[k]) for k in FIELDS_INT}
                dict_str = {k: str(row[k]) for k in FIELDS_STR}
                dict_float = {k: float(row[k]) for k in FIELDS_FLOAT}
                list_output.append({FIELD_TRADE_COUNT: 1, **dict_int, **dict_str, **dict_float})

    return list_output


def _extract_orders(dict_input):

    # create dataframe
    df = pd.DataFrame(dict_input)

    # calculating for each record the product of the volume times the price
    df[FIELD_VP] = df.loc[:, FIELD_TRADE_VOL] * df.loc[:, FIELD_TRADE_PRC]

    # FIRST AGGREGATION
    df_gby_1 = df.groupby(FIELDS_KEY)

    # for each order:
    # sum volume and price*volume
    # count the number of records
    # report the other fields
    df_tmp_1 = pd.concat([df_gby_1[FIELDS_SUM].sum(),
                          df_gby_1[FIELDS_REP].first()], axis=1)

    # resetting the index of the grouped dataframe
    # this operation transforms the index of a dataframe into a field
    df_grouped_1 = df_tmp_1.reset_index()

    # inferred vwap
    df_grouped_1[FIELD_VWAP] = df_grouped_1[FIELD_VP]/df_grouped_1[FIELD_TRADE_VOL]

    return df_grouped_1[FIELDS_OUTPUT].to_dict(orient='records')


def _add_date_time(ld_input):

    for trade in ld_input:

        # datetime format of the placement time
        dt_placement = datetime.datetime.strptime(trade[FIELD_xdtP], '%Y-%m-%d %H:%M:%S')

        # julian date of the placement time
        trade[FIELD_DATE] = dt_placement.toordinal()

        # minute from midnight of the placement time
        trade[FIELD_MIN_START] = dt_placement.hour*60 + dt_placement.minute

        # datetime format of the execution date
        dt_execution = datetime.datetime.strptime(trade[FIELD_xdtX], '%Y-%m-%d %H:%M:%S')

        # minute from midnight of the execution time
        trade[FIELD_MIN_END] = dt_execution.hour*60 + dt_execution.minute

    return ld_input


def _add_hash(ld_input):

    for trade in ld_input:
        # joining the fields in the key and generating the hash value
        trade[FIELD_HASH] = hash(''.join([str(trade[k]) for k in FIELDS_KEY]))

    return ld_input


if __name__ == "__main__":

    path_raw = '/Users/eliazarinelli/db/raw/tmp_07_01.txt.gz'
    path_stage = '/Users/eliazarinelli/db/stage/test.p'

    print('extracting dict...')
    tmp_0 = _import_dict(path_raw)

    print('extracting orders...')
    tmp_1 = _extract_orders(tmp_0)

    print('adding date and min...')
    tmp_2 = _add_date_time(tmp_1)

    print('adding hash...')
    tmp_3 = _add_hash(tmp_2)

    import pickle
    pickle.dump(tmp_3, open(path_stage, "wb" ) )

    print('done')

