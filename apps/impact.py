__author__ = 'eliazarinelli'

'''
An interactive visualisation of market impact.

Use the ``bokeh serve`` command to run the example by executing:
    bokeh serve impact.py
at your command prompt. Then navigate to the URL
    http://localhost:5006/impact
in your browser.
'''

import numpy as np

from bokeh.io import curdoc
from bokeh.layouts import row, column, widgetbox
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.models.widgets import TextInput, Select, Button, DatePicker, \
    CheckboxButtonGroup, RadioButtonGroup, RangeSlider

from bokeh.plotting import figure

import os
import sys
sys.path.insert(0, os.path.abspath('..'))
from dana.reservoir import OrdersReservoir

N_MINS_DAY = 60*24

def connect_db():

    # create engine url
    engine_url = 'mongodb://' + text_db_host.value + ':' + text_db_port.value + '/'

    # connection to the database
    api = OrdersReservoir(engine_url=engine_url, db_name=text_db_orders.value)

    api_global.append(api)

def update_data():

    if radio_sign.active == 0:
        filter_sign = None
    elif radio_sign.active == 1:
        filter_sign = 1
    else:
        filter_sign = -1

    # get impact
    dd_iterator = api_global[0].bucketed_stats(field_x='ci.pr_day',
                                               field_y='ci.impact_vv',
                                               boundaries=list(np.linspace(0., 1., 10)),
                                               sign=filter_sign,
                                               start_inf=range_start.range[0],
                                               start_sup=range_start.range[1],
                                               end_inf=range_end.range[0],
                                               end_sup=range_end.range[1],
                                               duration_ph_inf=range_duration.range[0],
                                               duration_ph_sup=range_duration.range[1],
                                               prp_inf=range_participation.range[0],
                                               prp_sup=range_participation.range[1])
    print(range_participation.range)
    dd = list(dd_iterator)

    xx = []
    yy = []
    for i in dd:
        xx.append(i['m_x'])
        yy.append(i['m_y'])

    source_impact.data = dict(x_0=xx, y_0=yy)



api_global = []

# Database connection
name_db_host = 'localhost'
text_db_host = TextInput(title='Database host:', value=name_db_host, width=400)

name_db_port = '27017'
text_db_port = TextInput(title='Database port:', value=name_db_port, width=400)

name_db_orders = 'orders_example'
text_db_orders = TextInput(title='Name database orders:', value=name_db_orders, width=400)

button_db_connect = Button(label="Connect to db")
button_db_connect.on_click(connect_db)

# Plot
plot_impact = figure(
    plot_height=300,
    plot_width=600,
    tools="crosshair,pan,reset,save,wheel_zoom,hover",
    x_range=[0, 1],
    y_range=[-1, 1],
    title="Market impact"
    )

# adding points to the plot
source_impact = ColumnDataSource(data=dict(x_0=[], y_0=[]))
plot_impact.circle(x='x_0', y='y_0', source=source_impact)

# Inputs
list_impact = ['end - start', 'vwap - start', 'vwap - vwap']
select_impact = Select(title="Impact:", value=list_impact[0], options=list_impact, width=60)

radio_sign = RadioButtonGroup(labels=["All", "Buy", "Sell"], active=0)

range_start = RangeSlider(start=0, end=N_MINS_DAY, range=(0, N_MINS_DAY), step=1, title="Start minute")
range_end = RangeSlider(start=0, end=N_MINS_DAY, range=(0, N_MINS_DAY), step=1, title="End minute")
range_duration = RangeSlider(start=0, end=N_MINS_DAY, range=(0, N_MINS_DAY), step=1, title="Duration")
range_participation = RangeSlider(start=0, end=1., range=(0, 1.), step=0.01, title="Participation Rate")

button_plot = Button(label="Plot")
button_plot.on_click(update_data)

# Output
inputs = widgetbox(children=[text_db_host, text_db_port, text_db_orders, button_db_connect,
                             select_impact, radio_sign, range_start, range_end, range_duration,
                             range_participation, button_plot], width=300)
curdoc().add_root(row(inputs, plot_impact))
curdoc().title = "Impact"