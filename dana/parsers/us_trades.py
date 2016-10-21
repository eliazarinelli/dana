import numpy as np 
import pandas as pd
import gzip
import csv

from constants import *


def _read_dictionary(path_input):

    # instantiation of an empty dictionary
    # the keys of the dict are the same keys of FIELDS_TO_KEEP
    dict_raw_data = {}
    for key in FIELDS_TO_KEEP.keys():
        dict_raw_data[key] = []

    # opening the gz file to read
    with gzip.open(path_input, 'rt') as file_gz:

        # reading the input file line by line
        reader = csv.DictReader(file_gz, delimiter='|')

        c = 0
        for row in reader:

            # check if all the elements are non-empty
            check_empty = all(v for v in row.values())

            # adding entries to the dict
            if check_empty:
                for field in FIELDS_TO_KEEP.keys():
                    dict_raw_data[field].append(row[field])

            # count
            c += 1
            if c % 100 == 0:
                print(c)

    # transform integer fields and float fields in np.array
    for k, v in FIELDS_TO_KEEP.items():
        if v in (int, float):
            dict_raw_data[k] = np.array(dict_raw_data[k], dtype=v)

    return dict_raw_data


def _extract_orders(dict_raw_data):


    # create dataframe
    df = pd.DataFrame(dict_raw_data)

    # calculating for each record the product of the volume times the price
    df[FIELD_VOL_PRICE[2]] = df.loc[:, FIELD_VOL_PRICE[0]] * df.loc[:, FIELD_VOL_PRICE[1]]

    # FIRST AGGREGATION
    df_gby_1 = df.groupby(GROUP_KEY_1)

    # for each order:
    # sum volume and price*volume
    # count the number of records
    # report the other fields
    df_tmp_1 = pd.concat([df_gby_1[GROUP_SUM_1].sum(),
                          df_gby_1[GROUP_SUM_1[0]].count(),
                          df_gby_1[GROUP_REP_1].first()], axis=1)


    # resetting the index of the grouped dataframe
    # this operation transforms the index of a dataframe into a field
    df_grouped_1 = df_tmp_1.reset_index()

    return df_grouped_1


def store_order(path_input, path_output):

    dict_raw_data = _read_dictionary(path_input)

    df = _extract_orders(dict_raw_data)

    store = pd.HDFStore(path_output)
    store['test_table'] = df
    store.close()

    return 0

if __name__ == "__main__":

    path_name = '/Users/eliazarinelli/Desktop/rebsq/stage'
    #file_name = 'Trades200702.txt.gz'
    file_name = 'pippo.txt.gz'
    path_input = path_name + '/' + file_name
    path_output = path_name + '/store.h5'

    store_order(path_input, path_output)


