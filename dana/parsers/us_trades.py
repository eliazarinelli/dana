import numpy as np 
import pandas as pd
import datetime
import gzip
import csv

from configurations import FIELD_COUNT, FIELD_VOL, FIELD_PRC, FIELD_VP, FIELD_VWAP
from configurations import FIELDS_INT, FIELDS_STR, FIELDS_FLOAT
from configurations import FIELDS_KEY, FIELDS_SUM, FIELDS_REP
from configurations import FIELDS_OUTPUT
from configurations import FIELD_TIME_PLACEMENT, FIELD_TIME_EXECUTION, FIELD_DATE, FIELD_MIN_START, FIELD_MIN_END
from configurations import FIELD_HASH


#from constants import FIELDS_TO_KEEP, FIELD_VOL_PRICE, GROUP_KEY_1, GROUP_SUM_1, GROUP_REP_1,\
#    FIELD_TIME_PLACEMENT, FIELD_TIME_EXECUTION, FIELD_DATE, FIELD_MIN_START, FIELD_MIN_END,\
#    FIELD_VWAP_INF, GROUPBY_RENAME, GROUPBY_OUTPUT, FIELDS_OUTPUT


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
                list_output.append({FIELD_COUNT: 1, **dict_int, **dict_str, **dict_float})

    return list_output


def _extract_orders(dict_input):

    # create dataframe
    df = pd.DataFrame(dict_input)

    # calculating for each record the product of the volume times the price
    df[FIELD_VP] = df.loc[:, FIELD_VOL] * df.loc[:, FIELD_PRC]

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
    df_grouped_1[FIELD_VWAP] = df_grouped_1[FIELD_VP]/df_grouped_1[FIELD_VOL]

    return df_grouped_1[FIELDS_OUTPUT].to_dict(orient='records')


def _add_date_time(ld_input):

    for trade in ld_input:

        # datetime format of the placement time
        dt_placement = datetime.datetime.strptime(trade[FIELD_TIME_PLACEMENT], '%Y-%m-%d %H:%M:%S')

        # julian date of the placement time
        trade[FIELD_DATE] = dt_placement.toordinal()

        # minute from midnight of the placement time
        trade[FIELD_MIN_START] = dt_placement.hour*60 + dt_placement.minute

        # datetime format of the execution date
        dt_execution = datetime.datetime.strptime(trade[FIELD_TIME_EXECUTION], '%Y-%m-%d %H:%M:%S')

        # minute from midnight of the execution time
        trade[FIELD_MIN_END] = dt_execution.hour*60 + dt_execution.minute

    return ld_input


def _add_hash(ld_input):

    for trade in ld_input:
        # joining the fields in the key and generating the hash value
        trade[FIELD_HASH] = hash(''.join([str(k) for k in FIELDS_KEY]))

    return ld_input

# OLD #####################################################

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
            if c % 100000 == 0:
                print(c)

    # transform integer fields and float fields in np.array
    for k, v in FIELDS_TO_KEEP.items():
        if v in (int, float):
            dict_raw_data[k] = np.array(dict_raw_data[k], dtype=v)

    return dict_raw_data



def _extract_orders_old(dict_raw_data):

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

    # rename columns to avoid double names
    tmp_columns = list(df_grouped_1.columns)
    tmp_columns[tmp_columns.index(GROUP_SUM_1[0])] = FIELD_RENAME[0]
    tmp_columns[tmp_columns.index(GROUP_SUM_1[0])] = FIELD_RENAME[1]
    df_grouped_1.columns = tmp_columns

    # inferred vwap
    df_grouped_1[FIELD_VWAP_INF] = df_grouped_1[GROUP_SUM_1[1]]/df_grouped_1[FIELD_RENAME[0]]

    # julian date and min from midnight
    dt_str = list(df_grouped_1[FIELD_TIME_PLACEMENT])
    dd = [datetime.datetime.strptime(dd, '%Y-%m-%d %H:%M:%S') for dd in dt_str]
    day = [d.toordinal() for d in dd]
    min_start = [d.hour*60 + d.minute for d in dd]

    dt_str = list(df_grouped_1[FIELD_TIME_EXECUTION])
    dd = [datetime.datetime.strptime(dd, '%Y-%m-%d %H:%M:%S') for dd in dt_str]
    min_end = [d.hour*60 + d.minute for d in dd]

    df_grouped_1[FIELD_DATE] = day
    df_grouped_1[FIELD_MIN_START] = min_start
    df_grouped_1[FIELD_MIN_END] = min_end


    return df_grouped_1


def store_order(path_input, path_output):

    dict_raw_data = _read_dictionary(path_input)

    print('extracting orders...')
    df = _extract_orders(dict_raw_data)

    dict_raw_data = None 

    print('sotring orders...')
    #store = pd.HDFStore(path_output)
    #store['test_table'] = df
    #store.close()
    df.to_pickle(path_output)
    print('done!')

    return 0

if __name__ == "__main__":
    if False:
        print('extracting dict...')
        dict_raw_data = _read_dictionary('/Users/eliazarinelli/Desktop/rebsq/stage/tmp_07_01.txt.gz')

        print('extracting orders...')
        df = _extract_orders(dict_raw_data)

    print('extracting dict...')
    tmp_0 = _import_dict('/Users/eliazarinelli/Desktop/rebsq/stage/tmp_07_01.txt.gz')

    print('extracting orders...')
    tmp_1 = _extract_orders(tmp_0)

    print('adding date and min...')
    tmp_2 = _add_date_time(tmp_1)

    print('adding hash...')
    tmp_3 = _add_hash(tmp_2)
    print('done')

