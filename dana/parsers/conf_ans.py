
# Set to true if you want to locally dump the output
BOOL_LOCAL_DUMP = True

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
