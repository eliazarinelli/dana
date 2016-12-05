
FIELDS_TO_KEEP = {'volume': int,
                  'Price' : float,
                  'xp': float,
                  'xv': int,
                  'clientcode': str,
                  'clientmgrcode': str,
                  'clientbkrcode': str,
                  'symbol': str,
                  'StockStyle': str,
                  'side': int,
                  'xdtP': str,
                  'xdtX': str,
                  'xpP': float,
                  'xpX': float,
                  'xpPX': float,
                  'xvPX': int,
                  'dpO': float,
                  'dpC': float,
                  'dpH': float,
                  'dpL': float,
                  'dpOC': float,
                  'dvOC': int}

FIELD_VOL_PRICE = ['volume', 'Price', 'vol_pr']
GROUP_KEY_1 = ['xp', 'xv', 'clientcode', 'clientmgrcode', 'clientbkrcode', 'symbol', 'side', 'xdtP', 'xdtX']
GROUP_SUM_1 = ['volume', 'vol_pr']
GROUP_REP_1 = ['xpP', 'xpX', 'xpPX', 'xvPX', 'dpO', 'dpC', 'dpH', 'dpL', 'dpOC', 'dvOC']
FIELD_TIME_PLACEMENT = 'xdtP'
FIELD_TIME_EXECUTION = 'xdtX'
FIELD_DATE = 'date'
FIELD_MIN_START = 'min_start'
FIELD_MIN_END = 'min_end'
FIELD_RENAME = ['sum_volume', 'count']
FIELDS_OUTPUT = ['clientcode', 'clientmgrcode', 'clientbkrcode', 'symbol', 'side']

