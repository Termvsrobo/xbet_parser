from pathlib import Path

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
