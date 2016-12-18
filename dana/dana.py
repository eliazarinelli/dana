from .userconfig import *

from .models import Order, DayInfo, PeriodInfo

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class Dana():

	def __init__(self):
		engine = create_engine('mysql+pymysql://' + USER_NAME + ':' + USER_PWD +
							   '@' + DB_HOST + '/' + DB_NAME)
		Session = sessionmaker()
		Session.configure(bind=engine)
		self._session = Session()

	def count_metaorders(self, symbol=None):
		if symbol is None:
			return self._session.query(Order.id).count()
		else:
			return self._session.query(Order.id).filter(Order.symbol==symbol).count()

