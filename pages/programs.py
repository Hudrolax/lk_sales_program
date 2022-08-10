import dash
from dash import dcc, html, dash_table, ctx, callback, Input, Output, State
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import pandas as pd
from data_methods import redis_worker, program_worker, date_options


dash.register_page(__name__, path='/', title='Установка планов продаж')


def fill_tbl(period=None, subdivision=None, region=None, manager=None, replace_program=False, **kwargs) -> (dict, list):
    db = kwargs.get('db', 0)
    if period is None:
        period = redis_worker[db].first_forecast_period()

    gfd = redis_worker[db].main_table(period, subdivision, region, manager)
    if gfd.empty:
        return {}, []
    gfd = gfd.groupby(by=['Группа', 'Прогноз', 'RMSE'], as_index=False).max()

    def round_forecast(x):
        if x < 0:
            return 0
        elif 5 > x > 0:
            return round(x, 3)
        elif 10 > x >= 5:
            return round(x, 2)
        elif 100 > x >= 10:
            return round(x, 1)
        elif x >= 100:
            return round(x, 0)
        else:
            return x

    gfd['Прогноз'] = gfd['Прогноз'].apply(round_forecast)
    gfd['RMSE'] = gfd['RMSE'].apply(round_forecast)

    gfd['План'] = 0
    gfd['Отклонение'] = 0

    df_program = program_worker.get_program(period, subdivision, region, manager)

    for i in range(len(gfd)):
        group = gfd.at[i, 'Группа']
        gfd.at[i, 'План'] = program_worker.plane(group, df_program)
        gfd.at[i, 'Отклонение'] = program_worker.dev(group, df_program)

    if replace_program:
        gfd['План'] = gfd['Прогноз']
        gfd['Отклонение'] = gfd['RMSE']

    def columns_iterator():
        for col in gfd.columns:
            if col != 'Ед':
                yield {"name": col, "id": col}

    tbl_columns = [record for record in columns_iterator()]
    tbl_columns[0]['name'] = ['', tbl_columns[0]['name']]
    for i in range(1, 3):
        tbl_columns[i]['name'] = [f'Прогноз', tbl_columns[i]['name']]
    for i in range(3, 5):
        tbl_columns[i]['name'] = [f'План 1С', tbl_columns[i]['name']]
        tbl_columns[i]['editable'] = True

    for i in range(1, 5):
        tbl_columns[i]['type'] = 'numeric'

    return gfd.to_dict('records'), tbl_columns


def send_program_to_1c(tbl_data: list, period, layer, subdivision=None, region=None, manager=None) -> str | None:
    programs = []
    for row in tbl_data:
        program = {
            'group': row['Группа'],
            'forecast': row['Прогноз'],
            'rmse': row['RMSE'],
            'program': row['План'],
            'deviation': row['Отклонение']
        }
        if subdivision is not None:
            program['subdivision'] = subdivision
        if region is not None:
            program['region'] = region
        if manager is not None:
            program['manager'] = manager

        programs.append(program)

    return program_worker.set_program(layer, period, programs)


layout = dbc.Container([
    # menu
    dbc.Row([
        dbc.Col(
            html.Div([
                "Прогноз на",
                dcc.Dropdown(id="prediction_date",
                             options=date_options(), value=date_options()[0]['value'],
                             clearable=False, persistence=True, persistence_type='session'),
            ]),
            width={'size': 2, 'offset': 0}
        ),

        dbc.Col(
            html.Div([
                html.Div('Разрез планирования'),
                dcc.Dropdown(id="forecast_layer", options=['В целом по компании', 'Подразделение', 'Регион',
                                                           'Менеджер'], value='В целом по компании', clearable=False,
                             persistence=True,
                             persistence_type='session'),
            ]), width={'size': 2, 'offset': 0}
        ),

        dbc.Col(
            html.Div([
                html.Div(id='forecast_layer_label'),
                dcc.Dropdown(id="layer", style={'display': 'none'}, searchable=True, clearable=False,
                             persistence=True, persistence_type='session'),
            ]), width={'size': 4, 'offset': 0}
        ),

        dbc.Col([
            dcc.ConfirmDialogProvider(
                children=html.Button('Прогноз в план', id='replace-btn', n_clicks=0,
                                     className='btn btn-outline-primary mx-2'),
                id='replace_confirmation_dialog',
                message='Перенести прогноз в план? Это перезатрет ручные изменения!'
            ),
        ], align='end', width={'size': 2, 'offset': 0}),

        dbc.Col([
            dcc.ConfirmDialogProvider(
                children=html.Button('Применить план', id='submit-btn', n_clicks=0,
                                     className='btn btn-outline-success mx-2'),
                id='send_confirmation_dialog',
                message='Установить планы продаж в 1С?'
            ),
        ], align='end', width={'size': 2, 'offset': 0})
    ]),

    dbc.Row(dbc.Col(html.Br())),

    dbc.Row([
        # основная таблица
        dbc.Col(
            dash_table.DataTable(
                *fill_tbl(),
                id='tbl',
                style_cell_conditional=[
                    {
                        'if': {'column_id': 'Группа'},
                        'textAlign': 'left',
                    }
                ],
                # style_data_conditional=[
                #     {
                #         'if': {
                #             'column_id': 'План',
                #             'filter_query': '{modified} == True',
                #         },
                #         'backgroundColor': 'dodgerblue',
                #         'color': 'white'
                #     }
                # ],
                style_header={
                    'backgroundColor': 'white',
                    'fontWeight': 'bold',
                    'textAlign': 'center'
                },
                merge_duplicate_headers=True,
            ),
            width={'size': 6, 'offset': 0}
        ),

        # Информация о модели прогнозирования
        dbc.Col([
            html.Div('Модель прогнозирования: prophet'),
            html.Div('Горизонт прогнозирования: 6 месяцев'),
            html.Div('Удалять выбросы: Да'),
            html.Div('Выбросами считаются точки, стоящие дальше, чем 1.5 * межквантильное расстояние от 25 и 75 персентилей.'),
            html.Div('Черные точки - исторические данные. Красные - выбросы в данных. Синяя линия - прогноз. Голубая область - стандартрное отклонение прогноза.'),
            html.Div('Пунктирные линии - теоретический максимум и минимум прогноза. Максимум берется, как +20% к максимуму истории (без учета выбросов)'),
            dcc.Graph(id='main-graph',
                      figure=redis_worker[0].main_graph()
                      ),
        ], width={'size': 6, 'offset': 0})
    ], justify='start'),
    dbc.Modal(
        [
            dbc.ModalHeader("Установка плана в 1С"),
            dbc.ModalBody(
                id='send_modal_body'
            ),
            dbc.ModalFooter(
                dbc.Button("Закрыть", id="close_send_modal", className="ml-auto")
            ),
        ], id="send_modal",
    ),
], fluid=True)


@callback(
    Output('layer', 'options'),
    Output('layer', 'style'),
    Output('forecast_layer_label', 'children'),
    Input('forecast_layer', 'value'),
    Input('db', 'value'),
)
def update_forecast_layers(forecast_layer, db):
    layer_label = forecast_layer
    style = {'display': 'block'}
    if forecast_layer == 'В целом по компании':
        layer_label = ''
        style = {'display': 'none'}
    return redis_worker[db].options(forecast_layer), style, layer_label


@callback(
    Output('tbl', 'data'),
    Output('tbl', 'columns'),
    Output("send_modal", "is_open"),
    Output('send_modal_body', 'children'),
    Output('submit-btn', 'disabled'),
    Input('prediction_date', 'value'),
    Input('forecast_layer', 'value'),
    Input('layer', 'value'),
    Input("close_send_modal", "n_clicks"),
    Input('send_confirmation_dialog', 'submit_n_clicks'),
    Input('replace_confirmation_dialog', 'submit_n_clicks'),
    Input('db', 'value'),
    State('tbl', 'data'),
)
def update_table(period, forecast_layer, layer, close_send_model_clicks, submit_n_clicks, replace_n_clicks, db, tbl_data):
    subdivision = None
    region = None
    manager = None
    if forecast_layer == 'Подразделение':
        subdivision = layer
    elif forecast_layer == 'Регион':
        region = layer
    elif forecast_layer == 'Менеджер':
        manager = layer
    if period is None:
        raise PreventUpdate
    kwargs = {'period': pd.to_datetime(period), "layer": forecast_layer, "subdivision": subdivision,
              'region': region, 'manager': manager, 'db': db}
    open_send_modal = False

    modal_body = ''
    replace_program = False
    if ctx.triggered_id == 'send_confirmation_dialog':
        if submit_n_clicks:
            # подтвердили отправку плана в 1С
            error = send_program_to_1c(tbl_data, **kwargs)
            if error is None:
                modal_body = 'Планы успешно установлены'
            else:
                modal_body = f'Ошибка: {error}'
            open_send_modal = True
    elif ctx.triggered_id == 'replace_confirmation_dialog':
        if replace_n_clicks:
            # подтвердили замену плана прогнозом
            replace_program = True

    send_btn_disabled = period is None or forecast_layer is None or (
            forecast_layer != 'В целом по компании' and layer is None)
    return *fill_tbl(replace_program=replace_program, **kwargs), open_send_modal, modal_body, send_btn_disabled


@callback(
    Output('main-graph', 'figure'),
    Input('tbl', 'active_cell'),
    Input('forecast_layer', 'value'),
    Input('layer', 'value'),
    Input('db', 'value'),
    State('tbl', 'data')
)
def update_graph(active_cell, forecast_layer, layer, db, table_data):
    if active_cell and active_cell['column_id'] == 'Группа':
        subdivision = None
        region = None
        manager = None
        if forecast_layer == 'Подразделение':
            subdivision = layer
        elif forecast_layer == 'Регион':
            region = layer
        elif forecast_layer == 'Менеджер':
            manager = layer

        row = active_cell['row']
        group = table_data[row][active_cell['column_id']]
        kwargs = {
            'group': group,
            'subdivision': subdivision,
            'region': region,
            'manager': manager
        }
        main_graph = redis_worker[db].main_graph(**kwargs)
        return main_graph
    else:
        raise PreventUpdate
