from declare_mo import Order, DayInfo, PeriodInfo
import pickle

if __name__ == '__main__':

    path_stage = "/Users/eliazarinelli/db/stage/test.p"
    order_list = pickle.load(open(path_stage, "rb"))

    from sqlalchemy import create_engine

    # no / at the beginning of the path
    path_db = 'Users/eliazarinelli/db/db_sqlite/test.db'
    engine = create_engine('sqlite:////' + path_db)

    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    for order in order_list:

        new_order = Order(
            id=order['hash_key'],
            client=order['clientcode'],
            mgr=order['clientmgrcode'],
            bkr=order['clientbkrcode'],
            symbol=order['symbol'],
            side=order['side'],
            date=order['date'],
            start=order['min_start'],
            end=order['min_end'],
            volume_inf=order['volume'],
            vwap_inf=order['vwap'],
            volume=order['xv'],
            vwap=order['xp'],
            cc=order['nn']
        )
        session.merge(new_order)
    session.commit()

    if False:
        new_dayinfo = DayInfo(
            id=1,
            volume=1000,
            p_vwap=3.,
            p_open=3.,
            p_close=4.,
            p_high=5.,
            p_low=2.,
            volatility=1.
        )




