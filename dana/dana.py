from .userconfig import *

from .models import Orders, DayInfo, PeriodInfo

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from sqlalchemy import func
class Dana():

	def __init__(self):
		engine = create_engine('mysql+pymysql://' + USER_NAME + ':' + USER_PWD +
							   '@' + HOST_NAME + '/' + DB_NAME)
		Session = sessionmaker()
		Session.configure(bind=engine)
		self._session = Session()

	def count_metaorders(self, symbol=None):
		if symbol is None:
			return self._session.query(Orders.id).count()
		else:
			return self._session.query(Orders.id).filter(Orders.symbol==symbol).count()

	def return_all(self):
		return self._session.query(func.avg(Orders.date + DayInfo.v_market)).\
			select_from(Orders).\
			join(DayInfo, and_(Orders.symbol == DayInfo.symbol, Orders.date == DayInfo.date)).scalar()

