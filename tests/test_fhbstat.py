import random
from pathlib import Path
from threading import Event

import numpy as np
import pytest

from base import BrowserManager
from config import settings
from parsers.fhbstat import (FHBParser, FHBStatFilter, FieldType, FloatField,
                             TimeField)


def test_page():
    file_path = Path(__file__).parent / Path('data') / Path('FHB_ Футбол Исход.html')
    assert file_path.exists()
    content = file_path.read_text()
    df = FHBParser.parse_content(content)
    head_df = FHBParser.parse_head_table(content)
    assert not df.empty
    assert not head_df.empty


@pytest.mark.parametrize(
    'value,round_to,result',
    [
        (1.55, '0.1', '1.5'),
        (1.55, '0.', '1.'),
        (1.55, '0', '1'),
        (2.05, '0.1', '2.0'),
        (2.05, '0.', '2.'),
        (2.05, '0', '2'),
        ('7.6', '0.1', '7.6'),
        ('7.655', '0.01', '7.65'),
        ('7.65', '0.01', '7.65')
    ]
)
def test_round(value, round_to, result):
    float_field = FloatField(type=FieldType.FLOAT, filter_value=round_to, column=22)
    if isinstance(value, str):
        value = float(value)
    value = float_field.get_value(value)
    assert value == result


@pytest.mark.parametrize(
    'value,round_to,result',
    [
        ('23:45', '00:00', '23:45'),
        ('23:45', '00:', '23:'),
        ('23:45', '00', '23'),
    ]
)
def test_round_datetime(value, round_to, result):
    time_field = TimeField(type=FieldType.TIME, filter_value=round_to, column=4)
    value = time_field.get_value(value)
    assert value == result


@pytest.mark.parametrize(
    'data,result',
    [
        (
            [
                {'25': 1.55, '26': 2.05, 'Количество матчей': 5},
                {'25': 2*1.55, '26': 2*2.05, 'Количество матчей': 6}
            ],
            {'25': 2.3955, '26': 3.1682}
        ),
        (
            [
                {'25': 1.55, '26': 2.05, 'Количество матчей': 5},
                {'25': 2*1.55, '26': 2*2.05, 'Количество матчей': 6, '32': 3.4}
            ],
            {'25': 2.3955, '26': 3.1682, '32': 1.8545}
        ),
        (
            [
                {'25': 1.55, '26': 2.05, 'Количество матчей': 5, '8': 'Общий этап'},
                {'25': 2*1.55, '26': 2*2.05, 'Количество матчей': 6, '32': 3.4}
            ],
            {'25': 2.3955, '26': 3.1682, '32': 1.8545}
        ),
        (
            [
                {'25': 1.55, '26': 2.05, 'Количество матчей': 5, '16': 0},
                {'25': 2*1.55, '26': 2*2.05, 'Количество матчей': 6, '32': 3.4, '16': 0}
            ],
            {'25': 2.3955, '26': 3.1682, '32': 1.8545}
        ),
        (
            [
                {'25': 1.55, '26': 2.05, 'Количество матчей': 5, '16': '0'},
                {'25': 2*1.55, '26': 2*2.05, 'Количество матчей': 6, '32': 3.4, '16': '0'}
            ],
            {'25': 2.3955, '26': 3.1682, '32': 1.8545}
        ),
        (
            [
                {'25': 1.55, '26': 2.05, 'Количество матчей': 5, '11': None},
                {'25': 2*1.55, '26': 2*2.05, 'Количество матчей': 6, '32': 3.4, '11': None}
            ],
            {'25': 2.3955, '26': 3.1682, '32': 1.8545}
        ),
        (
            [],
            {}
        )
    ]
)
def test_average(data, result):
    res = FHBParser.get_means(data)
    assert res.keys() == result.keys()
    for key in res:
        np.testing.assert_approx_equal(res[key], result[key])


@pytest.mark.parametrize(
    'data_means,data_match,result',
    [
        (
            {'25': 80.0, '26': 50.0},
            {'25': 1.30, '26': 2.580},
            {'25': 0.04, '26': 0.29}
        ),
    ]
)
def test_mathematical_expectation(data_means, data_match, result):
    res = FHBParser.get_mathematical_expectation(data_means, data_match)
    assert res.keys() == result.keys()
    for key in res:
        np.testing.assert_approx_equal(res[key], result[key])


@pytest.mark.parametrize(
    'data,target,file_name',
    [
        (
            [
                {
                    '1': 25,
                    '2': 234,
                    'index': 1,
                    'url': 'https://fhbstat.com/football?1=19&2=12&3=2025'
                },
            ],
            '/football',
            None,
        ),
        (
            [
                {
                    '1': 25,
                    '2': 234,
                    'index': 1,
                    'url': 'https://fhbstat.com/football_24?1=19&2=12&3=2025'
                },
            ],
            '/football_24',
            'test1'
        ),
        (
            [
                {
                    '1': 25,
                    '2': 234,
                    'index': 1,
                    'url': 'https://fhbstat.com/football_total?1=19&2=12&3=2025'
                },
            ],
            '/football_total',
            'test2'
        ),
        (
            [
                {
                    '1': 25,
                    '2': 234,
                    'index': 1,
                    'url': 'https://fhbstat.com/football?1=19&2=12&3=2025'
                },
            ],
            '/hockey',
            None,
        ),
        (
            [
                {
                    '1': 25,
                    '2': 234,
                    'index': 1,
                    'url': 'https://fhbstat.com/football_total?1=19&2=12&3=2025'
                },
            ],
            '/hockey_total',
            'test2'
        ),
        (
            [
                {
                    '1': 19,
                    '2': 12,
                    '3': 2025,
                    '4': '23:45',
                    '7': 'asdfasdf',
                    '8': 'qwerqwer',
                    '9': '[poipi]',
                    '10': 'zxcvzxcv',
                    'index': 1,
                    'url': 'https://fhbstat.com/hockey_24?1=19&2=12&3=2025',
                    **{str(i): random.uniform(0.2, 10.0) for i in range(30, 67)}
                },
                {
                    '1': 19,
                    '2': 12,
                    '3': 2025,
                    '4': '23:45',
                    '7': 'asdfasdf',
                    '8': 'qwerqwer',
                    '9': '[poipi]',
                    '10': 'zxcvzxcv',
                    'index': 1,
                    'url': 'https://fhbstat.com/hockey_24?1=19&2=12&3=2025',
                    **{str(i): random.uniform(0.2, 10.0) for i in range(30, 67)}
                },
                {
                    '1': 19,
                    '2': 12,
                    '3': 2025,
                    '4': '23:45',
                    '7': 'asdfasdf',
                    '8': 'qwerqwer',
                    '9': '[poipi]',
                    '10': 'zxcvzxcv',
                    'index': 1,
                    'url': 'https://fhbstat.com/hockey_24?1=19&2=12&3=2025',
                    **{str(i): random.uniform(0.2, 10.0) for i in range(30, 67)}
                },

            ],
            '/hockey_24',
            'test1'
        ),
    ]
)
def test_get_file_response(data, target, file_name):
    is_running = Event()
    fhbstat_parser = FHBParser(is_running=is_running)
    fhbstat_parser.start()
    if file_name:
        fhbstat_parser.file_name = file_name
    response = fhbstat_parser.get_file_response(
        data,
        target
    )
    fhbstat_parser.stop()
    assert response
    assert Path(response.path).exists()
    if file_name:
        assert response.filename == f'{file_name}.xlsx'
    Path(response.path).unlink()


@pytest.mark.parametrize(
    'target,file_name',
    [
        ('/hockey_24', 'test2')
    ]
)
def test_get_file_response_merge_cells(target, file_name):
    is_running = Event()
    fhbstat_parser = FHBParser(is_running=is_running)
    data = []
    fhbstat_parser.upload_filters_from_json(
        Path(__file__).parent / Path('data') / Path('П1 (футбол)  новый парсер.json')
    )
    for i in range(1, 10 + 1):
        for _ in range(len(fhbstat_parser.user_filters.root) + 1):
            data.append(
                {
                    '1': 19,
                    '2': 12,
                    '3': 2025,
                    '4': '23:45',
                    '7': 'asdfasdf',
                    '8': 'qwerqwer',
                    '9': '[poipi]',
                    '10': 'zxcvzxcv',
                    'index': i,
                    'url': 'https://fhbstat.com/hockey_24?1=19&2=12&3=2025',
                    'Количество матчей': random.randint(1, 10),
                    **{str(i): random.uniform(0.2, 10.0) for i in range(30, 67)}
                },
            )
        for sym in ('%', 'кф', 'мо'):
            data.append(
                {
                    '1': np.nan,
                    '2': np.nan,
                    '3': np.nan,
                    '4': np.nan,
                    '7': np.nan,
                    '8': np.nan,
                    '9': np.nan,
                    '10': np.nan,
                    'index': i,
                    'url': 'https://fhbstat.com/hockey_24?1=19&2=12&3=2025',
                    'Количество матчей': sym,
                    **{str(i): np.nan for i in range(30, 67)}
                }
            )
        for _ in range(fhbstat_parser.count_empty_rows):
            data.append(
                {
                    '1': np.nan,
                    '2': np.nan,
                    '3': np.nan,
                    '4': np.nan,
                    '7': np.nan,
                    '8': np.nan,
                    '9': np.nan,
                    '10': np.nan,
                    'index': i,
                    'url': 'https://fhbstat.com/hockey_24?1=19&2=12&3=2025',
                    **{str(i): np.nan for i in range(30, 67)}
                }
            )
    fhbstat_parser.start()
    if file_name:
        fhbstat_parser.file_name = file_name
    response = fhbstat_parser.get_file_response(
        data,
        target
    )
    fhbstat_parser.stop()
    assert response
    assert Path(response.path).exists()
    if file_name:
        assert response.filename == f'{file_name}.xlsx'


def test_fhbstat_filter():
    filter_instance = FHBStatFilter(
        filter_id=15,
        filters=[dict(type=FieldType.FLOAT, filter_value='0.1', priority=1, column=22)]
    )
    assert filter_instance
    assert filter_instance.filter_id == 15
    for filter_field in filter_instance.filters:
        assert filter_field.type is FieldType.FLOAT
        assert filter_field.filter_value == '0.1'
        assert filter_field.priority == 1
        assert filter_field.get_value(15.422) == '15.4'
        result = {0: '15.4', 1: '15.'}
        for i, next_value in enumerate(filter_field.next_value(15.422)):
            assert result.get(i) == next_value
            assert filter_field.filter_value == '0.1'
        assert i == 1


def test_add_user_filters():
    is_running = Event()
    fhbstat_parser = FHBParser(is_running=is_running)
    fhbstat_parser.add_user_filter(filter_value='0.1', priority=1, column=22)
    fhbstat_parser.add_user_filter(
        filter_value='0.01',
        priority=1,
        column=22,
        filter_id=fhbstat_parser.user_filters.root[0].filter_id
    )
    assert fhbstat_parser.user_filters.root[0].filters[0].filter_value == '0.01'


def test_change_priority_filters():
    is_running = Event()
    fhbstat_parser = FHBParser(is_running=is_running)
    fhbstat_parser.add_user_filter(filter_value='0.01', priority=1, column=22)
    fhbstat_parser.add_user_filter(priority=5, column=22, filter_id=fhbstat_parser.user_filters.root[0].filter_id)
    assert fhbstat_parser.user_filters.root[0].filters[0].filter_value == '0.01'
    assert fhbstat_parser.user_filters.root[0].filters[0].priority == 5


def test_wrong_user_filters():
    is_running = Event()
    fhbstat_parser = FHBParser(is_running=is_running)
    fhbstat_parser.add_user_filter(filter_id=15, filter_value='0.1', priority=1, column=22)
    with pytest.raises(ValueError):
        fhbstat_parser.add_user_filter(filter_id=15, filter_value=15, priority=-11, column=22)
        assert fhbstat_parser.user_filters.root[0].filters[0].filter_value == '0.01'


def test_user_filters():
    is_running = Event()
    fhbstat_parser = FHBParser(is_running=is_running)
    fhbstat_parser.add_user_filter(filter_id=15, filter_value='0.1', priority=1, column=22)
    for _filter in fhbstat_parser.user_filters.root:
        for sub_filter in _filter.filters:
            sub_filter.get_value(15)


@pytest.mark.parametrize(
    'url,filter_path',
    [
        (
            'https://fhbstat.com/football?1=16&2=02&3=2026',
            Path(__file__).parent / Path('data') / Path('download_filters.json')
        ),
        (
            'https://fhbstat.com/football_total?%D0%BC_9_%D0%BC%D1%83%D0%BD%D0%BA%D1%83%D0%B1=1&1=17&2=02&3=2026&F1_76=2&F1_77=1&F1_78=1',  # noqa:E501
            Path(__file__).parent / Path('data') / Path('ИТ1 (клубные) .json')
        ),
        (
            'https://fhbstat.com/football?%D0%BC_6_%D1%87%D0%B5%D0%BC%D0%BF=1&1=18&2=02&3=2026',
            Path(__file__).parent / Path('data') / Path('П1 (футбол)  новый парсер.json')
        ),
        (
            'https://fhbstat.com/football?%D0%BC_6_%D1%87%D0%B5%D0%BC%D0%BF=1&1=21&2=02&3=2026',
            Path(__file__).parent / Path('data') / Path('П1 (футбол) новые пробивки.json')
        )
    ]
)
@pytest.mark.asyncio
async def test_fhbstat_parser(url, filter_path):
    is_running = Event()
    fhbstat_parser = FHBParser(is_running=is_running)
    fhbstat_parser.target_urls['1'] = url
    fhbstat_parser.email = settings.TEST_FHBSTAT_USERNAME
    fhbstat_parser.password = settings.TEST_FHBSTAT_PASSWORD
    fhbstat_parser.upload_filters_from_json(filter_path)
    b_manager = BrowserManager(is_running=is_running, parser=fhbstat_parser)
    async with b_manager as browser:
        if browser:
            response = await b_manager.parse(browser)
            assert response
            print(response.path)
