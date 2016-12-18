from declare_mo import Order, DayInfo, PeriodInfo

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class Metaorders():

	def __init__(self):
		# no / at the beginning of the path!
		#path_db = 'Users/eliazarinelli/db/db_sqlite/test.db'
		#engine = create_engine('sqlite:////' + path_db)
		_user = 'root'
		# fake pwd
		_pwd = 'pwd'
		_host = 'localhost'
		_db = 'elia_tmp'
		engine = create_engine('mysql+pymysql://'+_user+':'+_pwd+'@'+_host+'/'+_db)
		Session = sessionmaker()
		Session.configure(bind=engine)
		self._session = Session()

	def count_metaorders(self, symbol=None):
		if symbol is None:
			return self._session.query(Order.id).count()
		else:
			return self._session.query(Order.id).filter(Order.symbol==symbol).count()

