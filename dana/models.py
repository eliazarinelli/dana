from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,  Float, BigInteger, SmallInteger

Base = declarative_base()


class Orders(Base):
    __tablename__ = 'orders'
    id = Column(BigInteger, primary_key=True)
    mgr_id = Column(String(250), nullable=False)
    bkr_id = Column(String(250), nullable=False)
    symbol = Column(String(250), nullable=False)
    side = Column(SmallInteger, nullable=False)
    date = Column(Integer, nullable=False)
    start_min = Column(SmallInteger, nullable=False)
    end_min = Column(SmallInteger, nullable=False)
    v_order = Column(Integer, nullable=False)
    p_vwap = Column(Float, nullable=False)
    n_trades = Column(SmallInteger, nullable=False)


class DayInfo(Base):
    __tablename__ = 'dayinfo'
    symbol = Column(String(250), primary_key=True)
    date = Column(String(250), primary_key=True)
    v_market = Column(Integer, nullable=False)
    p_vwap = Column(Float, nullable=False)
    p_open = Column(Float, nullable=False)
    p_close = Column(Float, nullable=False)
    p_high = Column(Float, nullable=False)
    p_low = Column(Float, nullable=False)
    volatility = Column(Float, nullable=True)


class PeriodInfo(Base):
    __tablename__ = 'periodinfo'
    id = Column(BigInteger, primary_key=True)
    v_market = Column(Integer, nullable=False)
    p_start = Column(Float, nullable=False)
    p_end = Column(Float, nullable=False)
    p_vwap = Column(Float, nullable=False)
