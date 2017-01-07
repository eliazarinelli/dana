import numpy as np 
import pandas as pd
import datetime
import gzip
import csv
import time
import pickle

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import IntegrityError


import os
import sys
sys.path.insert(0, os.path.abspath('..'))

from models import Base, Orders, DayInfo, PeriodInfo
from userconfig import *

from conf_ans import *

DBSession = scoped_session(sessionmaker())
engine = None

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

    dict_out = {}

    for trade in ld_input:
        # joining the fields in the key and generating the hash value
        hh = hash(''.join([str(trade[k]) for k in FIELDS_KEY]))
        trade[FIELD_HASH] = hh
        dict_out[hh] = trade

    return list(dict_out.values())


def _add_volatility(ld_input):

    for trade in ld_input:
        try:
            trade[FIELD_VOLA] = (trade[FIELD_dpH] - trade[FIELD_dpL])/(trade[FIELD_dpC] - trade[FIELD_dpO])
        except ZeroDivisionError:
            trade[FIELD_VOLA] = None

    return ld_input


def _adjust_client(ld_input):

    for trade in ld_input:
        trade[FIELD_MGR_ID] = '_'.join([trade[FIELD_clientcode], trade[FIELD_clientmgrcode]])
        trade[FIELD_BKR_ID] = '_'.join([trade[FIELD_clientcode], trade[FIELD_clientbkrcode]])

    return ld_input


def _dump_ld(ld_input):
    pickle.dump(ld_input, open(FILE_STAGE, "wb" ) )


def _init_sqlalchemy():

    global engine
    engine = create_engine(ENGINE_URL, echo=False)
    DBSession.remove()
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)

def _isolate_orders(ld_input):

    ld_out = []

    for order in ld_input:
        new_entry = {
            'id': order[ORDERS_NAME_MAP['id']],
            'mgr_id': order[ORDERS_NAME_MAP['mgr_id']],
            'bkr_id': order[ORDERS_NAME_MAP['bkr_id']],
            'symbol': order[ORDERS_NAME_MAP['symbol']],
            'side': order[ORDERS_NAME_MAP['side']],
            'date': order[ORDERS_NAME_MAP['date']],
            'start_min': order[ORDERS_NAME_MAP['start_min']],
            'end_min': order[ORDERS_NAME_MAP['end_min']],
            'v_order': order[ORDERS_NAME_MAP['v_order']],
            'p_vwap': order[ORDERS_NAME_MAP['p_vwap']],
            'n_trades': order[ORDERS_NAME_MAP['n_trades']]
        }

        ld_out.append(new_entry)
    return ld_out


def _write_order_to_db(ld_input):

    _init_sqlalchemy()

    ld_orders = _isolate_orders(ld_input)

    t0 = time.time()

    try:
        engine.execute(Orders.__table__.insert(), ld_orders)
        print(
            "SQLAlchemy Core: Total time for " + str(len(ld_orders)) +
            " records " + str(time.time() - t0) + " secs")

    except IntegrityError:
        print('bho')






def _write_to_db(ld_input):
        engine = create_engine(ENGINE_URL)
        Session = sessionmaker()
        Session.configure(bind=engine)
        session = Session()

        for order in ld_input:

            # check on consistency of the order volume and vwap
            if order[FIELD_ORDER_VOL] == order[FIELD_TRADE_VOL] \
                    and np.abs((order[FIELD_ORDER_PRC] - order[FIELD_VWAP])/order[FIELD_ORDER_PRC])<THRESHOLD_VWAP:
                new_order = Orders(
                    id=order[ORDERS_NAME_MAP['id']],
                    mgr_id=order[ORDERS_NAME_MAP['mgr_id']],
                    bkr_id=order[ORDERS_NAME_MAP['bkr_id']],
                    symbol=order[ORDERS_NAME_MAP['symbol']],
                    side=order[ORDERS_NAME_MAP['side']],
                    date=order[ORDERS_NAME_MAP['date']],
                    start_min=order[ORDERS_NAME_MAP['start_min']],
                    end_min=order[ORDERS_NAME_MAP['end_min']],
                    v_order=order[ORDERS_NAME_MAP['v_order']],
                    p_vwap=order[ORDERS_NAME_MAP['p_vwap']],
                    n_trades=order[ORDERS_NAME_MAP['n_trades']]
                )
                session.merge(new_order)

                new_periodinfo = PeriodInfo(
                    id=order[PERIODINFO_NAME_MAP['id']],
                    v_market=order[PERIODINFO_NAME_MAP['v_market']],
                    p_start=order[PERIODINFO_NAME_MAP['p_start']],
                    p_end=order[PERIODINFO_NAME_MAP['p_end']],
                    p_vwap=order[PERIODINFO_NAME_MAP['p_vwap']]
                )
                session.merge(new_periodinfo)

            new_dayinfo = DayInfo(
                symbol=order[DAYINFO_NAME_MAP['symbol']],
                date=order[DAYINFO_NAME_MAP['date']],
                v_market=order[DAYINFO_NAME_MAP['v_market']],
                p_vwap=order[DAYINFO_NAME_MAP['p_vwap']],
                p_open=order[DAYINFO_NAME_MAP['p_open']],
                p_close=order[DAYINFO_NAME_MAP['p_close']],
                p_high=order[DAYINFO_NAME_MAP['p_high']],
                p_low=order[DAYINFO_NAME_MAP['p_low']],
                volatility=order[DAYINFO_NAME_MAP['volatility']]
            )
            session.merge(new_dayinfo)




        session.commit()


if __name__ == "__main__":

    t_0 = time.time()
    print('extracting dict...')
    tmp_0 = _import_dict(FILE_RAW)

    print('extracting orders...')
    tmp_1 = _extract_orders(tmp_0)

    print('adding date and min...')
    _add_date_time(tmp_1)

    print('adding volatility...')
    _add_volatility(tmp_1)

    print('adjusting clientcode...')
    _adjust_client(tmp_1)

    print('adding hash...')
    tmp_2 = _add_hash(tmp_1)


    if BOOL_LOCAL_DUMP:
        print('dumping file...')
        _dump_ld(tmp_2)

    if BOOL_DB_COMMIT:
        print('writing to database...')
        _write_order_to_db(tmp_2)

    print('Total time (secs):')
    print(time.time()-t_0)

