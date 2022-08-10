import requests
from env import SERVER, BASE, SET_PROGRAM_ROUTE, USER, PASSWORD, API_KEY, GET_QUERY_ROUTE
from queries import GET_GENERAL_PROGRAM_QUERY, GET_SUBDIVISION_PROGRAM_QUERY, GET_REGION_PROGRAM_QUERY, \
    GET_MANAGER_PROGRAM_QUERY
from datetime import datetime
import logging
import json
import pandas as pd
import pdb
from plotly import io
from upgraded_redis import UpgradedRedis
from redis.exceptions import ConnectionError as RedisConnectionError, DataError
import plotly.express as px
from dateutil.relativedelta import relativedelta
from calendar import monthrange
import argparse
import sys


def keys(key):
    _keys = {
        'Подразделение': 'subdivision',
        'Регион': 'region',
        'Менеджер': 'manager'
    }
    _keys2 = {
        'subdivision': 'Подразделение',
        'region': 'Регион',
        'manager': 'Менеджер'
    }
    if _keys.get(key) is not None:
        return _keys.get(key)
    else:
        return _keys2.get(key)


def end_of_month(_date: datetime) -> datetime:
    last_day_of_month = monthrange(_date.year, _date.month)[1]
    return _date.replace(day=last_day_of_month, hour=23, minute=59, second=59, microsecond=9999)


def rus_month(_date: datetime) -> str:
    months = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь',
              'Декабрь']
    return f'{months[_date.month - 1]} {_date.strftime("%y")}'


def date_options() -> list:
    def months_generator():
        now = datetime.now()
        for i in range(0, 6):
            _date = now + relativedelta(months=i)
            yield {'label': rus_month(_date), 'value': _date}

    _options = [k for k in months_generator()]
    return _options


class RedisWorker(UpgradedRedis):
    logger = logging.getLogger('MyRedis')

    def get(self, *args, **kwargs):
        try:
            return super().get(*args, **kwargs)
        except (RedisConnectionError, DataError) as ex:
            self.logger.error(ex)
            return None

    def _get_graph(self, graph, group: str = '', **kwargs):
        key = f'prophet,{group},{kwargs.get("subdivision")},{kwargs.get("region")},{kwargs.get("manager")},{graph}'
        try:
            model = json.loads(self.get(key))
            return io.from_json(model['data'], skip_invalid=True)
        except (json.JSONDecodeError, ValueError, TypeError, KeyError) as ex:
            return px.scatter()

    def main_graph(self, group: str = '', **kwargs):
        return self._get_graph('graph', group, **kwargs)

    def graph_components(self, group: str = '', **kwargs):
        return self._get_graph('graph_component', group, **kwargs)

    def boxplot(self, group: str = '', **kwargs):
        return self._get_graph('boxplot', group, **kwargs)

    def main_table(self, period: datetime = None, subdivision=None, region=None, manager=None) -> pd.DataFrame:
        period = end_of_month(period)

        try:
            data_dict = json.loads(self.get(f'{period.strftime("%d.%m.%Y")},{subdivision},{region},{manager}'))
            _df = pd.DataFrame(data_dict).reset_index(drop=True)
            _df['Прогноз'] = _df['Прогноз'].fillna(0)
            _df['RMSE'] = _df['RMSE'].fillna(0)
            return _df
        except (json.JSONDecodeError, ValueError, TypeError) as ex:
            self.logger.error(ex)
            return pd.DataFrame()

    def first_forecast_period(self) -> datetime:
        period = self.last_timestamp()
        if period is not None:
            return end_of_month(period + relativedelta(months=1))
        else:
            return end_of_month(datetime.now())

    def last_timestamp(self) -> datetime.timestamp:
        try:
            return pd.to_datetime(self.get('actual_date').decode('utf-8'), infer_datetime_format=True)
        except (AttributeError, ValueError, TypeError, pd.errors.ParserError) as ex:
            return None

    def options(self, layer: str) -> list:
        def options_iterator(_actual_data):
            for k in _actual_data.sort_values(by='data')['data'].unique():
                if k != '':
                    yield k

        try:
            options = pd.DataFrame(json.loads(self.get(keys(layer)))['data'], columns=['data'])
            return [k for k in options_iterator(options)]
        except (json.JSONDecodeError, ValueError, TypeError, KeyError) as ex:
            return []


class ProgramWorker:
    logger = logging.getLogger('ProgramWorker')
    logger.level = logging.INFO

    def __init__(self):
        self.session = requests.Session()
        self.session.auth = (USER, PASSWORD)
        self.headers = {'Content-type': 'application/json; charset=UTF-8',
                        'Accept': 'text/plain'}

    @staticmethod
    def plane(group, _df: pd.DataFrame) -> float:
        """
        Возврщает план числом
        :param group: группа номенклатуры ЛК
        :param _df: dataframe в котором есть колонки Группа и План
        :return:
        """
        try:
            return _df.at[_df[_df['Группа'] == group].index[0], 'План']
        except (IndexError, KeyError):
            return 0

    @staticmethod
    def dev(group, _df: pd.DataFrame) -> float:
        """
        Возврщает отклонение числом
        :param group: группа номенклатуры ЛК
        :param _df: dataframe в котором есть колонки Группа и План
        :return:
        """
        try:
            return _df.at[_df[_df['Группа'] == group].index[0], 'Отклонение']
        except (IndexError, KeyError):
            return 0

    def set_program(self, layer: str, period: datetime, program: list) -> str | None:
        """
        :param period: datetime период прогноза
        :param layer: имя разреза установки плана "В целом по компании", "Подразделение", "Регион", "Менеджер"
        :param program: список словарей с данными
        :return: None if return code 200, str - if some error got
        """
        json_dict = {'layer': layer, 'period': period.strftime("%d.%m.%Y"), 'program': program}
        _route = f'http://{SERVER}/{BASE}{SET_PROGRAM_ROUTE}'
        response = self.session.post(_route, data=json.dumps(json_dict, indent=4, ensure_ascii=False).encode('utf-8'),
                                     headers=self.headers)
        if response.status_code == 200:
            return None
        else:
            return response.text

    def get_program(self, period: datetime, subdivision=None, region=None, manager=None) -> pd.DataFrame:
        if period is None:
            return pd.DataFrame()

        _route = f'http://{SERVER}/{BASE}{GET_QUERY_ROUTE}'
        _period = f'ДАТАВРЕМЯ({period.year}, {period.month}, 1, 0, 0, 0)'

        if subdivision is not None:
            _query = GET_SUBDIVISION_PROGRAM_QUERY.replace('&Период', _period).replace('&Подразделение', subdivision)
        elif region is not None:
            _query = GET_REGION_PROGRAM_QUERY.replace('&Период', _period).replace('&Регион', region)
        elif manager is not None:
            _query = GET_MANAGER_PROGRAM_QUERY.replace('&Период', _period).replace('&Менеджер', manager)
        else:
            _query = GET_GENERAL_PROGRAM_QUERY.replace('&Период', _period)
        json_dict = {'api_key': API_KEY, 'query': _query}

        response_text = ""
        try:
            response_text = self.session.post(_route, json=json_dict, headers=self.headers).text
            response_dict: dict = json.loads(response_text)
            return pd.json_normalize(response_dict['data'])
        except requests.exceptions.ConnectTimeout as ex:
            self.logger.error(f'{requests.exceptions.ConnectTimeout}: {ex}')
        except requests.exceptions.ConnectionError as ex:
            self.logger.error(f'{requests.exceptions.ConnectionError}: {ex}')
        except json.decoder.JSONDecodeError as ex:
            self.logger.error(f'{json.decoder.JSONDecodeError}: {ex};\n response_text: {response_text}')
        except KeyError as ex:
            self.logger.error(f'{KeyError}: {ex}')

        return pd.DataFrame()


def create_parser():
    _parser = argparse.ArgumentParser()
    _parser.add_argument('-rh', '--redis_host', default='192.168.19.18')
    _parser.add_argument('-rdb', '--redis_db', default='2')
    return _parser


parser = create_parser()
params = parser.parse_args(sys.argv[1:])

redis_worker = [
    RedisWorker(host=params.redis_host, db=0),
    RedisWorker(host=params.redis_host, db=1),
    RedisWorker(host=params.redis_host, db=2),
]

# redis_worker = RedisWorker(host=params.redis_host, db=params.redis_db)
program_worker = ProgramWorker()

if __name__ == '__main__':
    pw = ProgramWorker()
    date = datetime(2022, 7, 1)
    df = pw.get_program(date, subdivision='Краснодар, Тополиная, 27/1')
    print(df)
