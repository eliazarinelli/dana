
# Name of the input file
#FILE_RAW = '/Users/eliazarinelli/db/raw/ciao.txt.gz'
#FILE_RAW = '/Users/eliazarinelli/db/raw/tmp_07_01.txt.gz'
FILE_RAW = '/Users/eliazarinelli/db/raw/Trades200701.txt.gz'

# fields from ans
FIELD_ans_clientcode = 'clientcode'
FIELD_ans_clientmgrcode = 'clientmgrcode'
FIELD_ans_clientbkrcode = 'clientbkrcode'
FIELD_ans_symbol = 'symbol'
FIELD_ans_side = 'side'
FIELD_ans_trade_volume = 'volume'
FIELD_ans_trade_price = 'Price'
FIELD_ans_order_volume = 'xv'
FIELD_ans_order_price = 'xp'
FIELD_ans_datetime_start = 'xdtP'
FIELD_ans_datetime_end = 'xdtX'

# fields internal
FIELD_clientcode = 'client'
FIELD_clientmgrcode = 'mgr'
FIELD_clientbkrcode = 'bkr'
FIELD_symbol = 'symbol'
FIELD_side = 'side'
FIELD_trade_volume = 'trade_volume'
FIELD_trade_price = 'trade_price'
FIELD_order_volume = 'order_volume'
FIELD_order_price = 'order_price'
FIELD_datetime_start = 'datetime_start'
FIELD_datetime_end = 'datetime_end'

# internal -> ans field mapping
NAME_MAPPING_ANS = {
    FIELD_clientcode: FIELD_ans_clientcode,
    FIELD_clientmgrcode: FIELD_ans_clientmgrcode,
    FIELD_clientbkrcode: FIELD_ans_clientbkrcode,
    FIELD_symbol: FIELD_ans_symbol,
    FIELD_side: FIELD_ans_side,
    FIELD_trade_volume: FIELD_ans_trade_volume,
    FIELD_trade_price: FIELD_ans_trade_price,
    FIELD_order_volume: FIELD_ans_order_volume,
    FIELD_order_price: FIELD_ans_order_price,
    FIELD_datetime_start: FIELD_ans_datetime_start,
    FIELD_datetime_end: FIELD_ans_datetime_end
}

# fields to cast to int type
FIELD_int = [FIELD_side, FIELD_trade_volume, FIELD_order_volume]

# fields to cast to float type
FIELD_float = [FIELD_trade_price, FIELD_order_price]

# new fields
FIELD_date_julian = 'date'
FIELD_time_start = 'time_start'
FIELD_time_end = 'time_end'
FIELD_trade_vp = 'trade_vp'
FIELD_mgr = 'mgr'
FIELD_bkr = 'bkr'

# fields to keep
FIELDS_to_keep = [FIELD_bkr, FIELD_mgr, FIELD_symbol, FIELD_side,
                  FIELD_date_julian, FIELD_time_start, FIELD_time_end,
                  FIELD_order_price, FIELD_order_volume,
                  FIELD_trade_price, FIELD_trade_volume, FIELD_trade_vp]

# fields key of the groupy
FIELDS_KEY_GBY = [FIELD_bkr, FIELD_mgr, FIELD_symbol, FIELD_side,
                 FIELD_date_julian, FIELD_time_start, FIELD_time_end,
                 FIELD_order_price, FIELD_order_volume]

# sum fields for the groupy operation
FIELDS_SUM_GBY = [FIELD_trade_volume, FIELD_trade_vp]

# new fields
FIELD_count = 'count_trades'
FIELD_order_volume_inf = 'order_volume_inf'
FIELD_order_price_inf = 'order_price_inf'

ORDERS_NAME_MAP = {
    'mgr': FIELD_mgr,
    'bkr': FIELD_bkr,
    'symbol': FIELD_symbol,
    'side': FIELD_side,
    'date_exec': FIELD_date_julian,
    'time_start': FIELD_time_start,
    'time_end': FIELD_time_end,
    'volume': FIELD_order_volume,
    'price': FIELD_order_price,
    'n_trades': FIELD_count
}

THRESHOLD_VWAP = 0.0001
