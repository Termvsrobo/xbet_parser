from pathlib import Path

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
    ]
)
def test_round(value, round_to, result):
    value = FHBParser.round(value, round_to)
    assert value == result


@pytest.mark.parametrize(
    'data,result',
    [
        ([{'25': 1.55, '26': 2.05}, {'25': 2*1.55, '26': 2*2.05}], {'25': 2.325, '26': 3.075}),
        ([{'25': 1.55, '26': 2.05}, {'25': 2*1.55, '26': 2*2.05, '32': 3.4}], {'25': 2.325, '26': 3.075, '32': 3.4}),
        (
            [{'25': 1.55, '26': 2.05, '8': 'Общий этап'}, {'25': 2*1.55, '26': 2*2.05, '32': 3.4}],
            {'25': 2.325, '26': 3.075, '32': 3.4}
        ),
        (
            [{'25': 1.55, '26': 2.05, '16': 0}, {'25': 2*1.55, '26': 2*2.05, '32': 3.4, '16': 0}],
            {'25': 2.325, '26': 3.075, '32': 3.4, '16': 0}
        ),
        (
            [{'25': 1.55, '26': 2.05, '16': '0'}, {'25': 2*1.55, '26': 2*2.05, '32': 3.4, '16': '0'}],
            {'25': 2.325, '26': 3.075, '32': 3.4, '16': 0}
        ),
        (
            [{'25': 1.55, '26': 2.05, '11': None}, {'25': 2*1.55, '26': 2*2.05, '32': 3.4, '11': None}],
            {'25': 2.325, '26': 3.075, '32': 3.4, '11': np.float64('nan')}
        ),
    ]
)
def test_average(data, result):
    res = FHBParser.get_means(data)
    assert res.keys() == result.keys()
    for key in res:
        np.testing.assert_approx_equal(res[key], result[key])
