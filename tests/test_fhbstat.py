import random
from pathlib import Path
from threading import Event

import numpy as np
import pytest

from parsers.fhbstat import FHBParser


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
    if isinstance(value, str):
        value = float(value)
    value = FHBParser.round(value, round_to)
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
    value = FHBParser.round_datetime(value, round_to)
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
            {'25': 2.3955, '26': 3.1682, '32': 1.8545, '16': 0}
        ),
        (
            [
                {'25': 1.55, '26': 2.05, 'Количество матчей': 5, '16': '0'},
                {'25': 2*1.55, '26': 2*2.05, 'Количество матчей': 6, '32': 3.4, '16': '0'}
            ],
            {'25': 2.3955, '26': 3.1682, '32': 1.8545, '16': 0}
        ),
        (
            [
                {'25': 1.55, '26': 2.05, 'Количество матчей': 5, '11': None},
                {'25': 2*1.55, '26': 2*2.05, 'Количество матчей': 6, '32': 3.4, '11': None}
            ],
            {'25': 2.3955, '26': 3.1682, '32': 1.8545, '11': 0.0}
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
        ('/hockey_24', 'test1')
    ]
)
def test_get_file_response_merge_cells(target, file_name):
    is_running = Event()
    fhbstat_parser = FHBParser(is_running=is_running)
    data = []
    for i in range(1, 10 + 1):
        for _ in range(4):
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
                    **{str(i): random.uniform(0.2, 10.0) for i in range(30, 67)}
                },
            )
        for _ in range(7):
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
