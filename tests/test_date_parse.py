from datetime import datetime

import pytest
import pytz

from utils import parse_date_str


@pytest.mark.parametrize(
    'date,result',
    [
        (
            '06 сен 01:00',
            datetime(
                datetime.now(tz=pytz.timezone('Europe/Moscow')).year, 9, 6, 1, 0, 0, 0,
                tzinfo=pytz.timezone('Europe/Moscow')
            )
        ),
        (
            '1:00',
            datetime(
                datetime.now(tz=pytz.timezone('Europe/Moscow')).year,
                datetime.now(tz=pytz.timezone('Europe/Moscow')).month,
                datetime.now(tz=pytz.timezone('Europe/Moscow')).day,
                1,
                0,
                0,
                0,
                tzinfo=pytz.timezone('Europe/Moscow')
            )
        ),
    ]
)
def test_parse_russian_date(date, result):
    parsed_date = parse_date_str(date)

    assert parsed_date == result
