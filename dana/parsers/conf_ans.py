
# Set to true if you want to locally dump the output
BOOL_LOCAL_DUMP = False

# Set to true if you want to commit to the database the output
BOOL_DB_COMMIT = True

# Name of the input file
FILE_RAW = '/Users/eliazarinelli/db/raw/tmp_07_01.txt.gz'

# Name of the local dump file
FILE_STAGE = '/Users/eliazarinelli/db/stage/test.p'

# ##################################################################################

# Fields for volume and price
FIELD_SIDE = 'side'
FIELD_TRADE_VOL = 'volume'
FIELD_TRADE_PRC = 'Price'
FIELD_ORDER_VOL = 'xv'
FIELD_ORDER_PRC = 'xp'
FIELD_xdtP = 'xdtP'
FIELD_xdtX = 'xdtX'
FIELD_clientcode = 'clientcode'
FIELD_clientmgrcode = 'clientmgrcode'
FIELD_clientbkrcode = 'clientbkrcode'
FIELD_symbol = 'symbol'

FIELD_xvPX = 'xvPX'
FIELD_xpP = 'xpP'
FIELD_xpX = 'xpX'
FIELD_xpPX = 'xpPX'

FIELD_dvOC = 'dvOC'
FIELD_dpO = 'dpO'
FIELD_dpC = 'dpC'
FIELD_dpH = 'dpH'
FIELD_dpL = 'dpL'
FIELD_dpOC = 'dpOC'

# New fields, for counting and volume and price product
FIELD_TRADE_COUNT = 'nn'
FIELD_VP = 'vol_prc'
FIELD_VWAP = 'vwap'
FIELD_DATE = 'date'
FIELD_MIN_START = 'min_start'
FIELD_MIN_END = 'min_end'
FIELD_HASH = 'hash_key'
FIELD_VOLA = 'volatility'
FIELD_MGR_ID = 'mgr_id'
FIELD_BKR_ID = 'bkr_id'

# Fields for reading input
FIELDS_INT = [FIELD_SIDE, FIELD_TRADE_VOL, FIELD_ORDER_VOL, FIELD_xvPX, FIELD_dvOC]
FIELDS_STR = [FIELD_clientcode, FIELD_clientmgrcode, FIELD_clientbkrcode, FIELD_symbol, FIELD_xdtP, FIELD_xdtX]
FIELDS_FLOAT = [FIELD_TRADE_PRC, FIELD_ORDER_PRC, FIELD_xpP, FIELD_xpX, FIELD_xpPX,
                FIELD_dpO, FIELD_dpC, FIELD_dpH, FIELD_dpL, FIELD_dpOC]

# Fields for the group-by operation
FIELDS_KEY = [FIELD_ORDER_PRC, FIELD_ORDER_VOL, FIELD_clientcode, FIELD_clientmgrcode, FIELD_clientbkrcode,
              FIELD_symbol, FIELD_SIDE, FIELD_xdtP, FIELD_xdtX]
FIELDS_SUM = [FIELD_TRADE_VOL, FIELD_VP, FIELD_TRADE_COUNT]
FIELDS_REP = [FIELD_xpP, FIELD_xpX, FIELD_xpPX, FIELD_xvPX, FIELD_dpO,
              FIELD_dpC, FIELD_dpH, FIELD_dpL, FIELD_dpOC, FIELD_dvOC]

# Fields output
FIELDS_OUTPUT = FIELDS_KEY + FIELDS_REP + [FIELD_TRADE_COUNT, FIELD_TRADE_VOL, FIELD_VWAP]

# ##################################################################################

THRESHOLD_VWAP = 0.001

ORDERS_NAME_MAP = {
    'id': FIELD_HASH,
    'mgr_id': FIELD_MGR_ID,
    'bkr_id': FIELD_BKR_ID,
    'symbol': FIELD_symbol,
    'side': FIELD_SIDE,
    'date': FIELD_DATE,
    'start_min': FIELD_MIN_START,
    'end_min': FIELD_MIN_END,
    'v_order': FIELD_ORDER_VOL,
    'p_vwap': FIELD_ORDER_PRC,
    'n_trades': FIELD_TRADE_COUNT
}

DAYINFO_NAME_MAP = {
    'symbol': FIELD_symbol,
    'date': FIELD_DATE,
    'v_market': FIELD_dvOC,
    'p_vwap': FIELD_dpOC,
    'p_open': FIELD_dpO,
    'p_close': FIELD_dpC,
    'p_high': FIELD_dpH,
    'p_low': FIELD_dpL,
    'volatility': FIELD_VOLA
}

PERIODINFO_NAME_MAP = {
    'id': FIELD_HASH,
    'v_market': FIELD_xvPX,
    'p_start': FIELD_xpP,
    'p_end': FIELD_xpX,
    'p_vwap': FIELD_xpPX
}

# ##############################################################

# Name of the input file
#FILE_RAW = '/Users/eliazarinelli/db/raw/ciao.txt.gz'
FILE_RAW = '/Users/eliazarinelli/db/raw/tmp_07_01.txt.gz'

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