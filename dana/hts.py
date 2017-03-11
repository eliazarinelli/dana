
from pymongo import MongoClient

N_MINS_DAY = 24*60

TS_FIELDS = set(['date', 'mins', 'price', 'volume'])


class Hts(object):

    def __init__(self, engine_url, db_name):
        self._client = MongoClient(engine_url)
        self._db_name = db_name
        self._db_hts = self._client[self._db_name]

    def drop_ts(self, symbol=None):

        """
        Drop the ts database

        Input:
        -----
        symbol: string, if provided drop only the associated collection
        """

        if symbol is None:
            self._client.drop_database(self._db_name)
        else:
            self._db_hts.drop_collection(symbol)

    def insert_ts(self, symbol, ts):

        """
        Insert a time series in the symbol collection

        Input:
        ------
        symbol: string, the collection
        ts: dict, the time series

        """

        if set(ts[0].keys()) != TS_FIELDS:
            raise ValueError('Wrong format of input in ts: dict keys should be: ' + ', '.join(TS_FIELDS))

        collection_symbol = self._db_hts.get_collection(symbol)
        collection_symbol.insert_many(ts)

    def get_ts(self, symbol, date, min_start=0, min_end=N_MINS_DAY):

        """ Get the time series corresponding to a symbol and date """

        # get the collection corresponding to the symbol
        collection_symbol = self._db_hts.get_collection(symbol)

        # get the cursor filtering on the date, min_start and min_end
        # sort by the minutes
        cc = collection_symbol.find(
            {'date': date,
             'mins': {'$gte': min_start, '$lt': min_end}},
            {"_id": 0}).sort([('mins', 1)])

        return list(cc)

    def ts_index(self, symbol):

        # get the collection corresponding to the symbol
        collection_symbol = self._db_hts.get_collection(symbol)
        collection_symbol.create_index([('date', pymongo.ASCENDING)])

    def get_candle(self, symbol, date, min_start=0, min_end=N_MINS_DAY):

        """ Get the candle corresponding to a symbol and date """

        # get the collection corresponding to the symbol
        collection_symbol = self._db_hts.get_collection(symbol)

        # get the cursor filtering on the date, min_start and min_end
        # sort by the minutes
        pipeline = [
            {'$match': {'date': date, 'mins': {'$gte': min_start, '$lt': min_end}}},
            {'$sort': {'mins': 1}},
            {'$project': {'price': 1, 'volume': 1, 'pv': {'$multiply': ['$price', '$volume']}}},
            {'$group': {
                '_id': '',
                'open': {'$first': '$price'},
                'high': {'$max': '$price'},
                'low': {'$min': '$price'},
                'close': {'$last': '$price'},
                'volume': {'$sum': '$volume'},
                'pv': {'$sum': '$pv'}
                }}
        ]

        cc = list(collection_symbol.aggregate(pipeline))

        if len(cc) == 0:
            return {}
        else:
            candle = cc[0]
            candle['vwap'] = float(candle['pv'])/float(candle['volume'])
            del candle['_id']
            del candle['pv']
            return candle
