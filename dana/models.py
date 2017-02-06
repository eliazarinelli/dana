from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,  Float, BigInteger, SmallInteger

Base = declarative_base()


class Orders(Base):
    __tablename__ = 'orders'
    mgr = Column(String(250), primary_key=True)
    bkr = Column(String(250), primary_key=True)
    symbol = Column(String(250), primary_key=True)
    side = Column(SmallInteger, primary_key=True, autoincrement=False)
    date_exec = Column(Integer, primary_key=True, autoincrement=False)
    time_start = Column(SmallInteger, primary_key=True, autoincrement=False)
    time_end = Column(SmallInteger, primary_key=True, autoincrement=False)
    volume = Column(Integer, primary_key=True, autoincrement=False)
    price = Column(Float, primary_key=True)
    n_trades = Column(SmallInteger, nullable=False)


class Candle(Base):
    __tablename__ = 'dayinfo'
    symbol = Column(String(250), primary_key=True)
    date = Column(Integer, primary_key=True, autoincrement=False)
    time_start = Column(SmallInteger, primary_key=True, autoincrement=False)
    time_end = Column(SmallInteger, primary_key=True, autoincrement=False)
    p_open = Column(Float, nullable=False)
    p_high = Column(Float, nullable=False)
    p_low = Column(Float, nullable=False)
    p_close = Column(Float, nullable=False)
    p_vwap = Column(Float, nullable=False)
    v_market = Column(Integer, nullable=False)
