import dash
from dash import html, dcc, callback, Input, Output, State, ctx
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import pandas as pd
from data_methods import date_options, redis_worker, program_worker, keys
from datetime import datetime

dash.register_page(__name__, title='Администрирование')


def send_all_programs_to_1c(period: datetime) -> str | None:
    if period is None:
        period = redis_worker.first_forecast_period()

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

    layers = ['В целом по компании', 'Подразделение', 'Регион', 'Менеджер']
    for layer in layers:
        if layer == 'В целом по компании':
            programs = []
            gfd = redis_worker.main_table(period, None, None, None)
            gfd = gfd.groupby(by=['Группа', 'Прогноз', 'RMSE'], as_index=False).max()
            gfd['Прогноз'] = gfd['Прогноз'].apply(round_forecast)
            gfd['RMSE'] = gfd['RMSE'].apply(round_forecast)
            for i in range(len(gfd)):
                program = {
                    'group': gfd.at[i, 'Группа'],
                    'forecast': gfd.at[i, 'Прогноз'],
                    'rmse': gfd.at[i, 'RMSE'],
                    'program': gfd.at[i, 'Прогноз'],
                    'deviation': gfd.at[i, 'RMSE']
                }
                programs.append(program)
            result = program_worker.set_program(layer, period, programs)
            if result is not None:
                return result
        else:
            options = redis_worker.options(layer)
            for option in options:
                kwargs = {
                    'subdivision': None,
                    'region': None,
                    'manager': None
                }
                kwargs[keys(layer)] = option
                programs = []
                gfd = redis_worker.main_table(period, **kwargs)
                gfd = gfd.groupby(by=['Группа', 'Прогноз', 'RMSE'], as_index=False).max()
                gfd['Прогноз'] = gfd['Прогноз'].apply(round_forecast)
                gfd['RMSE'] = gfd['RMSE'].apply(round_forecast)
                for i in range(len(gfd)):
                    program = {
                        'group': gfd.at[i, 'Группа'],
                        'forecast': gfd.at[i, 'Прогноз'],
                        'rmse': gfd.at[i, 'RMSE'],
                        'program': gfd.at[i, 'Прогноз'],
                        'deviation': gfd.at[i, 'RMSE']
                    }
                    program[keys(layer)] = option
                    programs.append(program)
                result = program_worker.set_program(layer, period, programs)
                if result is not None:
                    return result
        # elif layer == 'Регион':
        #     options = redis_worker.options(layer)
        #     for option in options:
        #         programs = []
        #         gfd = redis_worker.main_table(period, None, option, None)
        #         gfd = gfd.groupby(by=['Группа', 'Прогноз', 'RMSE'], as_index=False).max()
        #         gfd['Прогноз'] = gfd['Прогноз'].apply(round_forecast)
        #         gfd['RMSE'] = gfd['RMSE'].apply(round_forecast)
        #         for i in range(len(gfd)):
        #             program = {
        #                 'group': gfd.at[i, 'Группа'],
        #                 'forecast': gfd.at[i, 'Прогноз'],
        #                 'rmse': gfd.at[i, 'RMSE'],
        #                 'program': gfd.at[i, 'Прогноз'],
        #                 'deviation': gfd.at[i, 'RMSE']
        #             }
        #             program['region'] = option
        #             programs.append(program)
        #         result = program_worker.set_program(layer, period, programs)
        #         if result is not None:
        #             return result
        # elif layer == 'Менеджер':
        #     options = redis_worker.options(layer)
        #     for option in options:
        #         programs = []
        #         gfd = redis_worker.main_table(period, None, None, option)
        #         gfd = gfd.groupby(by=['Группа', 'Прогноз', 'RMSE'], as_index=False).max()
        #         gfd['Прогноз'] = gfd['Прогноз'].apply(round_forecast)
        #         gfd['RMSE'] = gfd['RMSE'].apply(round_forecast)
        #         for i in range(len(gfd)):
        #             program = {
        #                 'group': gfd.at[i, 'Группа'],
        #                 'forecast': gfd.at[i, 'Прогноз'],
        #                 'rmse': gfd.at[i, 'RMSE'],
        #                 'program': gfd.at[i, 'Прогноз'],
        #                 'deviation': gfd.at[i, 'RMSE']
        #             }
        #             program['manager'] = option
        #             programs.append(program)
        #         result = program_worker.set_program(layer, period, programs)
        #         if result is not None:
        #             return result
    return None


layout = dbc.Container([
    dbc.Row([
        dbc.Col(
            html.Div([
                "План на",
                dcc.Dropdown(id="plan_date",
                             options=date_options(), value=date_options()[0]['value'],
                             clearable=False, persistence=True, persistence_type='session'),
            ]),
            width={'size': 2, 'offset': 0}
        ),

        dbc.Col(
            dcc.ConfirmDialogProvider(
                children=html.Button('Перезаписать все планы прогнозами', id='send-btn-admin', n_clicks=0,
                                     className='btn btn-outline-primary mx-2'),
                id='send_plans_admin',
                message='Перенести все прогнозы в план? Это перезатрет уже установленные прогнозы на выбранный период!'
            ), align='end'
        ),
    ]),

    dbc.Spinner(
        dbc.Modal(
            [
                dbc.ModalHeader("Установка плана в 1С"),
                dbc.ModalBody(
                    id='send_modal_body_admin'
                ),
                dbc.ModalFooter(
                    dbc.Button("Закрыть", id="close_send_modal_admin", className="ml-auto")
                ),
            ], id="send_modal_admin",
        ),
    )
])


@callback(
    Output("send_modal_admin", "is_open"),
    Output("send_modal_body_admin", 'children'),
    Input("send_plans_admin", "submit_n_clicks"),
    Input("close_send_modal_admin", "n_clicks"),
    State('plan_date', 'value'),
)
def send_plans(send_plans_clicks, close_modal_btn, plans_date):
    if plans_date is None:
        raise PreventUpdate
    open_send_modal = False
    modal_body = ''
    if ctx.triggered_id == 'send_plans_admin':
        if send_plans_clicks:
            # подтвердили отправку планов в 1С
            error = send_all_programs_to_1c(pd.to_datetime(plans_date))
            if error is None:
                modal_body = 'Планы успешно установлены'
            else:
                modal_body = f'Ошибка: {error}'
            open_send_modal = True
    return open_send_modal, modal_body
