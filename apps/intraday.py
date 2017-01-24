'''
An interactive visualisation of intraday orders.

Use the ``bokeh serve`` command to run the example by executing:
    bokeh serve intraday.py
at your command prompt. Then navigate to the URL
    http://localhost:5006/intraday
in your browser.
'''

from datetime import datetime

from bokeh.io import curdoc
from bokeh.layouts import row, column, widgetbox
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.models.widgets import TextInput, Select, Button, DatePicker
from bokeh.plotting import figure

import os
import sys
sys.path.insert(0, os.path.abspath('..'))

from dana.dana import Dana

# connection to the database
db_path = os.path.abspath('../db/db')
db_name = 'example.db'
engine_url = '/'.join(['sqlite:///', db_path, db_name])

# retrieve data
api = Dana(engine_url)
list_symbols = api.get_symbols()
list_dates = api.get_dates()

# plot orders definition
p_orders = figure(
    plot_height=400,
    plot_width=500,
    tools="crosshair,pan,reset,save,wheel_zoom,hover",
    x_range=[500, 1100],
    y_range=[-200, 200],
    title="Intraday orders"
    )

# plot orders select
p_orders.select_one(HoverTool).tooltips = [
    ("Broker", "@bkr"),
    ("Manager", "@mgr"),
    ("VWAP", "@price")
]

# definition of data source and markers for the orders
source_orders = ColumnDataSource(data=dict(x_0=[], y_0=[], x_1=[], y_1=[], color=[], bkr=[], mgr=[], price=[]))
p_orders.quad(left='x_0', bottom='y_0', right='x_1', top='y_1', color='color', source=source_orders, alpha=0.2)


# Set up widgets
w_text_db = TextInput(title='Database URL', value=engine_url, width=400)
w_select_symbol = Select(title="Symbol:", value=list_symbols[0], options=list_symbols, width=60)
w_datepickler = DatePicker(
    title='Date',
    min_date=datetime.fromordinal(list_dates[0]),
    max_date=datetime.fromordinal(list_dates[-1]),
    value=datetime.fromordinal(list_dates[0]),
    width=100)
w_button = Button(label="Update plot")


def update_data():

    # get the orders in the database corresponing to the selected date date and symbol
    dd = api.get_day(symbol=w_select_symbol.value, date=datetime.toordinal(w_datepickler.value))

    # get the data for the plot
    x_start = [order['time_start'] for order in dd]
    x_end = [order['time_end'] for order in dd]
    y_start = [0 if order['side'] == 1 else
               order['side']*float(order['volume'])/
               (1+order['time_end']-order['time_start']) for order in dd]
    y_end = [0 if order['side'] == -1 else
               order['side']*float(order['volume'])/
               (1+order['time_end']-order['time_start']) for order in dd]
    tmp = {1: 'navy', -1: 'red'}
    c_lines = [tmp[i['side']] for i in dd]
    bkr_list = [order['bkr'] for order in dd]
    mgr_list = [order['mgr'] for order in dd]
    price_list = [order['price'] for order in dd]

    # update the data in the plot
    source_orders.data = dict(x_0=x_start, y_0=y_start, x_1=x_end, y_1=y_end,
                              color=c_lines, bkr=bkr_list, mgr=mgr_list, price=price_list)

    # update the scale
    p_orders.y_range.start = min(y_start)
    p_orders.y_range.end = max(y_end)

    p_orders.title.text = 'Intraday orders - symbol: ' + w_select_symbol.value +\
                          '  date: ' + str(w_datepickler.value)[0:10]

# associate the function to the button
w_button.on_click(update_data)


# Set up layouts and add to document
inputs = widgetbox(children=[w_text_db, w_select_symbol, w_datepickler, w_button], width=300)

curdoc().add_root(row(p_orders, inputs))
curdoc().title = "Intraday"
