from program_worker import ProgramWorker
from datetime import datetime
import pandas as pd
import pytest
import pdb
import logging

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def test_program_worker():
    return ProgramWorker()


def test_get_program(test_program_worker):
    date = datetime(2022, 7, 1)
    df_general = test_program_worker.get_program(date)
    assert type(df_general) == pd.DataFrame
    assert 'Группа' in df_general.columns
    assert 'План' in df_general.columns
    assert 'Отклонение' in df_general.columns

    df_subdivision = test_program_worker.get_program(date, subdivision='Краснодар, Тополиная, 27/1')
    assert type(df_subdivision) == pd.DataFrame
    assert 'Группа' in df_subdivision.columns
    assert 'План' in df_subdivision.columns
    assert 'Отклонение' in df_subdivision.columns

    df_region = test_program_worker.get_program(date, region='Направление Краснодар+15км - Динской район')
    assert type(df_region) == pd.DataFrame
    assert 'Группа' in df_region.columns
    assert 'План' in df_region.columns
    assert 'Отклонение' in df_region.columns

    df_manager = test_program_worker.get_program(date, manager='Кибиткин Анатолий Игоревич')
    assert type(df_manager) == pd.DataFrame
    assert 'Группа' in df_manager.columns
    assert 'План' in df_manager.columns
    assert 'Отклонение' in df_manager.columns
