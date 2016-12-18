from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,  Float, BigInteger

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

Base = declarative_base()


class Order(Base):
    __tablename__ = 'order'
    id = Column(BigInteger, primary_key=True)
    client = Column(String(250), nullable=False)
    mgr = Column(String(250), nullable=False)
    bkr = Column(String(250), nullable=False)
    symbol = Column(String(250), nullable=False)
    side = Column(Integer, nullable=False)
    date = Column(Integer, nullable=False)
    start = Column(Integer, nullable=False)
    end = Column(Integer, nullable=False)
    volume = Column(Integer, nullable=False)
    vwap = Column(Float, nullable=False)
    volume_inf = Column(Integer, nullable=False)
    vwap_inf = Column(Float, nullable=False)
    cc = Column(Integer, nullable=False)


class DayInfo(Base):
    __tablename__ = 'dayinfo'
    id = Column(BigInteger, primary_key=True)
    volume = Column(Integer, nullable=False)
    p_vwap = Column(Float, nullable=False)
    p_open = Column(Float, nullable=False)
    p_close = Column(Float, nullable=False)
    p_high = Column(Float, nullable=False)
    p_low = Column(Float, nullable=False)
    #volatility = Column(Float, nullable=False)


class PeriodInfo(Base):
    __tablename__ = 'periodinfo'
    id = Column(BigInteger, primary_key=True)
    volume = Column(Integer, nullable=False)
    p_start = Column(Float, nullable=False)
    p_end = Column(Float, nullable=False)
    vwap = Column(Float, nullable=False)

if __name__ == '__main__':

    # no / at the beginning of the path!
    #path_db = 'Users/eliazarinelli/db/db_sqlite/test.db'
    _user = 'root'
    # fake pwd
    _pwd = 'pwd'
    _host = 'localhost'
    _db = 'elia_tmp'
    engine = create_engine('mysql+pymysql://'+_user+':'+_pwd+'@'+_host+'/'+_db)
    session = sessionmaker()
    session.configure(bind=engine)

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


