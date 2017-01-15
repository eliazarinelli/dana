from .userconfig import *

from .models import Orders, DayInfo, PeriodInfo

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from sqlalchemy import func


class Dana(object):

    def __init__(self, engine_url):

        engine = create_engine(engine_url)
        Session = sessionmaker()
        Session.configure(bind=engine)
        self._session = Session()

    def count_orders(self, symbol=None):

        if symbol is None:
            return self._session.query(Orders.mgr).count()
        else:
            return self._session.query(Orders.mgr).filter(Orders.symbol==symbol).count()

    def get_day(self, symbol=None, date=None):

        output = []

        record_list = self._session.query(Orders).\
            filter(and_(Orders.symbol == symbol, Orders.date_exec == date))

        for record in record_list:
            order = {
                'mgr': record.mgr,
                'bkr': record.bkr,
                'symbol': record.symbol,
                'side': record.side,
                'date_exec': record.date_exec,
                'time_start': record.time_start,
                'time_end': record.time_end,
                'volume': record.volume,
                'price': record.price,
                'n_trades': record.n_trades
            }

            output.append(order)

        return output


