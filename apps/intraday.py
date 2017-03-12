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
from dana.reservoir import OrdersReservoir


def connect_db():

    # create engine url
    engine_url = 'mongodb://' + text_db_host.value + ':' + text_db_port.value + '/'

    # connection to the database
    api = OrdersReservoir(engine_url=engine_url, db_name=text_db_orders.value)

    # get available symbols and dates
    list_symbols = api.list_symbols()
    list_dates_0 = api.list_dates()
    list_dates = [str(i) for i in list_dates_0]

    # display new available symbols and dates
    select_symbol.options = ['None'] + list_symbols
    select_date.options = ['None'] + list_dates

    api_global.append(api)

def update_data():

    # get the orders in the database corresponding to the selected date date and symbol
    symbol_in = select_symbol.value
    date_in = int(select_date.value)

    # get orders
    dd_iterator = api_global[0].get_orders(symbol_list=[symbol_in], date_list=[date_in])

    dd = list(dd_iterator)

    # get the data for the plot
    x_start = [order['min_start'] for order in dd]
    x_end = [order['min_end'] for order in dd]
    y_start = [0 if order['sign'] == 1 else
               order['sign']*float(order['volume'])/
               (order['min_end']-order['min_start']) for order in dd]
    y_end = [0 if order['sign'] == -1 else
               order['sign']*float(order['volume'])/
               (order['min_end']-order['min_start']) for order in dd]
    tmp = {1: 'navy', -1: 'red'}
    c_lines = [tmp[i['sign']] for i in dd]
    bkr_list = [order['bkr'] for order in dd]
    mgr_list = [order['mgr'] for order in dd]
    price_list = [order['price'] for order in dd]

    # update the data in the plot
    source_orders.data = dict(x_0=x_start, y_0=y_start, x_1=x_end, y_1=y_end,
                              color=c_lines, bkr=bkr_list, mgr=mgr_list, price=price_list)

    # update the scale
    plot_orders.y_range.start = min(y_start)
    plot_orders.y_range.end = max(y_end)
    plot_orders.x_range.start = min(x_start)
    plot_orders.x_range.end = max(x_end)

    plot_orders.title.text = 'Intraday orders - symbol: ' + select_symbol.value +\
                          '  date: ' + select_date.value

api_global = []

# Database connection
name_db_host = 'localhost'
text_db_host = TextInput(title='Database host:', value=name_db_host, width=400)

name_db_port = '27017'
text_db_port = TextInput(title='Database port:', value=name_db_port, width=400)

name_db_orders = 'orders_example'
text_db_orders = TextInput(title='Name database orders:', value=name_db_orders, width=400)


# Symbol and date selection

list_symbols = ['None']
select_symbol = Select(title="Symbol:", value='Pippo', options=list_symbols, width=60)

list_dates = ['None']
select_date = Select(title="Date:", value='Pippo', options=list_dates, width=60)


button_db_connect = Button(label="Connect to db")
button_db_connect.on_click(connect_db)

# Plot
plot_orders = figure(
    plot_height=300,
    plot_width=600,
    tools="crosshair,pan,reset,save,wheel_zoom,hover",
    x_range=[500, 1100],
    y_range=[-200, 200],
    title="Intraday orders"
    )
source_orders = ColumnDataSource(data=dict(x_0=[], y_0=[], x_1=[], y_1=[], color=[], bkr=[], mgr=[], price=[]))
plot_orders.quad(left='x_0', bottom='y_0', right='x_1', top='y_1', color='color', source=source_orders, alpha=0.2)

# plot orders select
plot_orders.select_one(HoverTool).tooltips = [
    ("Broker", "@bkr"),
    ("Manager", "@mgr"),
    ("VWAP", "@price")
]

button_plot = Button(label="Plot")
button_plot.on_click(update_data)

inputs = widgetbox(children=[text_db_host, text_db_port, text_db_orders, button_db_connect,
                             select_symbol, select_date, button_plot], width=300)
curdoc().add_root(row(inputs, plot_orders))
curdoc().title = "Intraday"
