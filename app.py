import dash.exceptions
import pandas as pd
from dash import Dash, html
import dash_bootstrap_components as dbc
from dash import dcc, html, dash_table, ctx
from data_worker import DataWorker
import logging
from auth import enable_dash_auth
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from datetime import datetime
from dateutil.relativedelta import relativedelta
from time import sleep

WRITE_LOG_TO_FILE = False
LOG_FORMAT = '%(name)s (%(levelname)s) %(asctime)s: %(message)s'
LOG_LEVEL = logging.WARNING

logger = logging.getLogger('main')

if WRITE_LOG_TO_FILE:
    logging.basicConfig(filename='dash.txt', filemode='w', format=LOG_FORMAT, level=LOG_LEVEL,
                        datefmt='%d/%m/%y %H:%M:%S')
else:
    logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL, datefmt='%d/%m/%y %H:%M:%S')


def rus_month(date: datetime) -> str:
    months = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь',
              'Декабрь']
    return f'{months[date.month - 1]} {date.strftime("%y")}'


db = DataWorker()
db.run()
print('waiting while data loads')
while db.df_s.empty or db.models.models == []:
    sleep(0.01)

app = Dash(__name__, title='Установка плана продаж', external_stylesheets=[dbc.themes.MINTY],
           meta_tags=[{"name": "viewport",
                       'content': 'width=device-width, initial-scale=1.0'}],
           url_base_pathname='/sales_program/',
           )
enable_dash_auth(app)
server = app.server


def fill_tbl(df: pd.DataFrame, period: datetime, subdivision=None, region=None) -> (dict, list):
    if subdivision is None and region is None:
        df = df.groupby(by=['Период', 'Группа'], as_index=False).sum()
    elif subdivision != 'компания':
        df = df[df['Подразделение'] == subdivision].groupby(by=['Период', 'Группа'], as_index=False).sum()
    elif region is not None:
        df = df[df['Регион'] == region].groupby(by=['Период', 'Группа'], as_index=False).sum()

    last1 = df['Период'].max()
    last2 = last1 - relativedelta(months=1)
    last3 = last2 - relativedelta(months=1)

    df_tbl = df.groupby('Группа', as_index=False).sum()
    df_tbl[rus_month(last3)] = 0
    df_tbl[rus_month(last2)] = 0
    df_tbl[rus_month(last1)] = 0
    df_tbl['Прогноз'] = 0
    df_tbl['Отклонение'] = 0
    df_tbl['План 1C'] = 0
    df_tbl['Отклонение 1C'] = 0
    df_tbl = df_tbl.drop('Показатель', axis=1).sort_values(by='Группа')

    def _volume(_group, date) -> float:
        series = df[(df['Группа'] == _group) & (df['Период'].dt.month == date.month) & \
                    (df['Период'].dt.year == date.year)]['Показатель']
        if len(series) == 1:
            return round(float(series), 3)
        else:
            return 0.

    def _forecast(**kwargs) -> float:
        models = db.models
        forecast_df = models.get_model(**kwargs).forecast
        if len(forecast_df) > 0:
            return round(
                float(forecast_df[(forecast_df['ds'].dt.year == period.year) & (forecast_df['ds'].dt.month == period.month)]['yhat']))
        else:
            return 0.

    for i in range(0, len(df_tbl)):
        group = df_tbl.at[i, 'Группа']
        df_tbl.at[i, rus_month(last3)] = _volume(group, last3)
        df_tbl.at[i, rus_month(last2)] = _volume(group, last2)
        df_tbl.at[i, rus_month(last1)] = _volume(group, last1)
        df_tbl.at[i, 'Прогноз'] = _forecast(group=group, subdivision=subdivision, region=region)
        df_tbl.at[i, 'Отклонение'] = db.models.get_model(group=group, subdivision=subdivision, region=region).std

    return df_tbl.to_dict('records'), [{"name": i, "id": i} for i in df_tbl.columns]


def date_options() -> list:
    def months_generator():
        now = datetime.now()
        for i in range(0, 6):
            date = now + relativedelta(months=i)
            yield {'label': rus_month(date), 'value': date}
    options = [k for k in months_generator()]
    return options


def subdivision_options() -> list:
    return [k for k in db.df_s['Подразделение'].unique()]


app.layout = dbc.Container([
    # dcc.Location(id='url', refresh=False),

    # header
    dbc.Row(
        dbc.Col(
            html.H3('Установка плана продаж',
                    className='text-center')
        )
    ),

    # menu
    dbc.Row([
        dbc.Col(
            html.Div([
                "Прогноз на",
                dcc.Dropdown(id="prediction_date", options=date_options()),
            ]),
            width={'size': 1, 'offset': 0}
        ),
        dbc.Col(
            html.Div([
                html.Div('Подразделение'),
                dcc.Dropdown(id="subdivision", options=subdivision_options()),
            ]), width={'size': 3, 'offset': 0}
        ),
    ]),

    dbc.Row(dbc.Col(html.Br())),

    dbc.Row([
        dbc.Col(
            dash_table.DataTable(*fill_tbl(db.df_s, db.df_s['Период'].max() + relativedelta(months=1))),
            width={'size': 8, 'offset': 0}
        ),
    ], justify='center'),

    dbc.Row(
        dbc.Col(
            dcc.Interval(
                id='interval-component',
                interval=30 * 1000,  # in milliseconds
                n_intervals=0
            )
        )
    )
], fluid=True)


@app.callback([Output('tbl', 'data'),
               Output('tbl', 'columns'),
               ],
              [
                  Input('prediction_date', 'value')
              ])
def update_output(date_value):
    print(date_value)
    raise PreventUpdate


if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0')
