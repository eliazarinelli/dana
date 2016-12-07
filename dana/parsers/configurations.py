# Fields for volume and price
FIELD_VOL = 'volume'
FIELD_PRC = 'Price'

# New fields, for counting and volume and price product
FIELD_COUNT = 'nn'
FIELD_VP = 'vol_prc'
FIELD_VWAP = 'vwap'

# Fields for reading input
FIELDS_INT = ['side', 'volume', 'xv', 'xvPX', 'dvOC']
FIELDS_STR = ['clientcode', 'clientmgrcode', 'clientbkrcode', 'symbol', 'xdtP', 'xdtX']
FIELDS_FLOAT = ['Price', 'xp', 'xpP', 'xpX', 'xpPX', 'dpO', 'dpC', 'dpH', 'dpL', 'dpOC']

# Fields for the group-by operation
FIELDS_KEY = ['xp', 'xv', 'clientcode', 'clientmgrcode', 'clientbkrcode', 'symbol', 'side', 'xdtP', 'xdtX']
FIELDS_SUM = [FIELD_VOL, FIELD_VP, FIELD_COUNT]
FIELDS_REP = ['xpP', 'xpX', 'xpPX', 'xvPX', 'dpO', 'dpC', 'dpH', 'dpL', 'dpOC', 'dvOC']

# Fields output
FIELDS_OUTPUT = FIELDS_KEY + FIELDS_REP + [FIELD_COUNT, FIELD_VOL, FIELD_VWAP]

# Fields for transforming date and time
FIELD_TIME_PLACEMENT = 'xdtP'
FIELD_TIME_EXECUTION = 'xdtX'
FIELD_DATE = 'date'
FIELD_MIN_START = 'min_start'
FIELD_MIN_END = 'min_end'

# Field hash
FIELD_HASH = 'hash_key'