import data_worker
import logging
import pandas as pd
import pytest
import pdb

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def test_setup():
    dw = data_worker.DataWorker()
    return dw


def test_get_data(test_setup):
    """
    Тест получения сырых данных из 1С
    :param test_setup:
    :return:
    """
    data = test_setup.get_data(test_setup.session, test_setup.logger)
    assert data['data'] != []


def test_load_data_from_1c(test_setup):
    """
    Тест загрузки в датафрейм данных из 1С
    :param test_setup:
    :return:
    """
    test_setup.load_data()
    assert type(test_setup._df) == pd.DataFrame
    assert 'Период' in test_setup._df.columns
    assert 'Показатель' in test_setup._df.columns
    assert 'Подразделение' in test_setup._df.columns
    assert 'Группа' in test_setup._df.columns
    assert not test_setup._df.empty
    test_setup.preprocessing_data()


def test_load_data_from_cache(test_setup):
    """
    Тест загрузки данных из файлового кеша
    :param test_setup:
    :return:
    """
    if test_setup.load_data_from_cache():
        assert type(test_setup._df) == pd.DataFrame
        assert 'Период' in test_setup._df.columns
        assert 'Показатель' in test_setup._df.columns
        assert 'Подразделение' in test_setup._df.columns
        assert 'Группа' in test_setup._df.columns
        assert not test_setup._df.empty
        test_setup.preprocessing_data()


def test_get_empty_model(test_setup):
    """
    Тест возврата пустой модели, если не найдена модель для запрошенных данных
    :param test_setup:
    :return:
    """
    model = test_setup.models.get_model(group='Неизвестная группа', subdivision='Непонятное подразделение')
    assert model.name is None
    assert type(model.forecast) == pd.DataFrame
    assert model.forecast.at[0, 'yhat'] == 0
    assert model.std == 999999
    assert model.rmse == 999999


def test_make_fit_predict_raw_data(test_setup):
    """
    Тест создания модели на основе выборки из сырых данных
    :param test_setup:
    :return:
    """
    df_group = test_setup.df_s.groupby(['Период', 'Группа'], as_index=False).sum()
    df_group = df_group[df_group['Группа'] == df_group['Группа'].unique()[0]]

    df_subdivision = test_setup.df_s.groupby(['Период', 'Группа', 'Подразделение'], as_index=False).sum()
    df_subdivision = df_subdivision[(df_subdivision['Группа'] == df_subdivision['Группа'].unique()[0]) \
                                    & (df_subdivision['Подразделение'] == df_subdivision['Подразделение'].unique()[0])]

    # модели в общем по-группам
    test_setup.models.make_fit_predict_raw_data(df_group)

    # модели в разрезе подразделений
    test_setup.models.make_fit_predict_raw_data(df_subdivision)

    # # модели в разрезе регионов
    # test_setup.models.make_fit_predict_raw_data(df_region)

    assert len(test_setup.models.models) > 0
    assert test_setup.models.models[0].name is not None
    assert test_setup.models.models[0].std > 0
    assert type(test_setup.models.models[0].forecast) == pd.DataFrame
    forecast = test_setup.models.models[0].forecast
    assert forecast.at[len(forecast) - 1, 'yhat'] != 0
