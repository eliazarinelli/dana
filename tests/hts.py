__author__ = 'eliazarinelli'

def generate_ts(date, start, end, v_max):

    price = 100.
    ts = []
    for mins in range(start, end):
        record = {
            'date': date,
            'mins': mins,
            'price': price,
            'volume': v_max
                 }
        ts.append(record)
        price += 1

    return ts
