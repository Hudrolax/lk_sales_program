import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
from data_methods import redis_worker

dash.register_page(__name__, title='Выполнение плана')


def fill_tbl(period=None, subdivision=None, region=None, manager=None):
    if period is None:
        period = redis_worker.first_forecast_period()

    gfd = redis_worker.main_table(period, subdivision, region, manager)

    def columns_iterator(df):
        for col in gfd.columns:
            if col != 'Ед':
                yield {"title": col, "field": col}

    tbl_columns = [record for record in columns_iterator(gfd)]
    return gfd.to_dict(), tbl_columns


layout = dbc.Container([
    dbc.Row([
        dbc.Col(
            html.Div([
                html.Div('Выполнение плана'),
                dcc.Dropdown(id="forecast_layer2", options=['В целом по компании', 'Подразделение', 'Регион',
                                                           'Менеджер'], value='В целом по компании', clearable=False,
                             persistence=True,
                             persistence_type='session'),
            ]), width={'size': 2, 'offset': 0}
        ),

        dbc.Col(
            html.Div([
                html.Div(id='forecast_layer_label2'),
                dcc.Dropdown(id="layer2", style={'display': 'none'}, searchable=True, clearable=False,
                             persistence=True, persistence_type='session'),
            ]), width={'size': 4, 'offset': 0}
        ),
    ]),
    dbc.Row([

    ]),
])


@callback(
    Output('layer2', 'options'),
    Output('layer2', 'style'),
    Output('forecast_layer_label2', 'children'),
    Input('forecast_layer2', 'value'),
)
def update_forecast_layers(forecast_layer):
    layer_label = forecast_layer
    style = {'display': 'block'}
    if forecast_layer == 'В целом по компании':
        layer_label = ''
        style = {'display': 'none'}
    return redis_worker.options(forecast_layer), style, layer_label
