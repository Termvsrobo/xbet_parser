from datetime import datetime

import pytest

from utils import parse_date_str


@pytest.mark.parametrize(
    'date,result',
    [
        ('06 сен 01:00', datetime(datetime.now().year, 9, 6, 1, 0, 0, 0)),
        ('1:00', datetime(datetime.now().year, datetime.now().month, datetime.now().day, 1, 0, 0, 0)),
    ]
)
def test_parse_russian_date(date, result):
    parsed_date = parse_date_str(date)

    assert parsed_date == result
