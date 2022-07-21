from datetime import datetime
import threading
from threading import Lock
from env import SERVER, BASE, GET_QUERY_ROUTE, API_KEY, USER, PASSWORD, PRODUCTION
import requests
import json
from time import sleep
import logging
import pandas as pd
from queries import SALES_DATA_QUERY
from models import Models


class DataWorker:
    logger = logging.getLogger('DataWorker')
    logger.level = logging.INFO

    def __init__(self):
        # датафрейм для загрузки данных
        self._df = pd.DataFrame([], columns=['Группа', 'Период', 'Показатель', 'Подразделение'])

        # датафрейм для хранения очищенных данных
        self._df_s = pd.DataFrame([], columns=['Группа', 'Период', 'Показатель', 'Подразделение'])

        self.models = Models()

        self.lock = Lock()
        self.update_data_thread = threading.Thread(target=self.threaded_func, args=(), daemon=True)

        self.session = requests.Session()
        self.session.auth = (USER, PASSWORD)

    @property
    def df_s(self) -> pd.DataFrame:
        """
        :return: df история по подразделениям без текущего месяца
        """
        with self.lock:
            _df = self._df_s.copy()
        return _df

    @staticmethod
    def _date_to_str_1c(_date: datetime) -> str:
        return _date.strftime("%d.%m.%Y %H:%M:%S")

    @staticmethod
    def get_data(session, logger) -> dict:
        """
        Получает из 1С историю продаж в разрезе групп и подразделений помесячно
        :param session: сессия для подключения к 1C
        :param logger: логгер
        :return: dict с историей продаж для загрузки в dataframe
        """
        _empty_response = {"data": []}

        headers = {'Content-type': 'application/json',
                   'Accept': 'text/plain'}
        json_dict = {'api_key': API_KEY, 'query': SALES_DATA_QUERY}
        logger.debug(f'json_dict: {json_dict}')
        try:
            _route = f'http://{SERVER}/{BASE}{GET_QUERY_ROUTE}'
            logger.debug(f'route: {_route}')
            response_text = session.post(_route, json=json_dict, headers=headers).text
            # logger.debug(f'response_text: {response_text}')
            return json.loads(response_text)
        except requests.exceptions.ConnectTimeout as ex:
            logger.error(f'{requests.exceptions.ConnectTimeout}: {ex}')
        except requests.exceptions.ConnectionError as ex:
            logger.error(f'{requests.exceptions.ConnectionError}: {ex}')
        except json.decoder.JSONDecodeError as ex:
            logger.error(f'{json.decoder.JSONDecodeError}: {ex}')
        except Exception as ex:
            logger.critical(f'_get_data: {ex}')
            raise ex

        return _empty_response

    def load_data(self) -> None:
        """
        Загружает историю продаж из 1С в dataframe
        :return:
        """
        _json_dict: dict = self.get_data(self.session, self.logger)
        try:
            _df = pd.json_normalize(_json_dict['data'])
            _df['Период'] = pd.to_datetime(_df['Период'], format='%d.%m.%Y %H:%M:%S')
            _df = _df.sort_values(by='Период', ascending=True)

            with self.lock:
                self._df = _df.copy()
            self._df.to_csv('history.csv', index=False)
            self.logger.debug('data updated')
        except KeyError as ex:
            self.logger.error(f'{KeyError}: {ex}')

    def load_data_from_cache(self) -> bool:
        """
        Функция загружает историю из кеша
        :return:
        """
        with self.lock:
            try:
                self._df = pd.read_csv('history.csv')
                self._df['Период'] = pd.to_datetime(self._df['Период'])
            except FileNotFoundError:
                pass
        return True

    def preprocessing_data(self) -> None:
        """
        Выполняет препроцессинг загруженных данных.
        Обрезка лишних периодов, выбросов.
        :return:
        """
        self._df_s = self._df[self._df['Период'] < datetime.now().replace(day=1, hour=0, minute=0, second=0, )]

    def _predict(self) -> None:
        df_group = self._df_s.groupby(['Период', 'Группа'], as_index=False).sum()
        df_subdivision = self._df_s.groupby(['Период', 'Группа', 'Подразделение'], as_index=False).sum()
        # df_region = self._df_s.groupby(['Период', 'Группа', 'Регион'], as_index=False).sum()

        if not PRODUCTION:
            df_group = df_group[df_group['Группа'] == df_group['Группа'].unique()[0]]
            df_subdivision = df_subdivision[(df_subdivision['Группа'] == df_subdivision['Группа'].unique()[0]) \
                                            & (df_subdivision['Подразделение'] ==
                                               df_subdivision['Подразделение'].unique()[0])]
            # df_region = df_region[:3]

        # модели в общем по-группам
        self.models.make_fit_predict_raw_data(df_group)

        # модели в разрезе подразделений
        self.models.make_fit_predict_raw_data(df_subdivision)

        # # модели в разрезе регионов
        # self.models.make_fit_predict_raw_data(df_region)

    def _load_preprocessing_predict(self) -> None:
        self.load_data()
        self.preprocessing_data()
        self._predict()

    def threaded_func(self):
        """
        Функция работает в отдельном потоке. Получает и обрабатывает данные истории продаж из 1С
        :return:
        """
        while True:
            if self._df.empty or self._df['Период'].max() < datetime.now():
                self._load_preprocessing_predict()
            sleep(86400)

    def run(self):
        self.load_data_from_cache()
        self.preprocessing_data()
        self._predict()
        self.update_data_thread.start()
