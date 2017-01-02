
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

NAME_MAP = {
    'orders_id': 'hash_key',
    'orders_mgr_id': 'mgr_id',
    'orders_bkr_id': 'bkr_id',
    'orders_symbol': 'symbol',
    'orders_side': 'side',
    'orders_date': 'date',
    'orders_start_min': 'min_start',
    'orders_end_min': 'min_end',
    'orders_v_order': 'xv',
    'orders_p_vwap': 'xp',
    'orders_n_trades': 'nn'
}
