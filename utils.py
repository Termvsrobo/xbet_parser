import calendar
import locale
from pathlib import Path

import yaml
from dateutil.parser import parse, parserinfo


def parse_date_str(date: str):
    old_locale = locale.getlocale()
    locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')

    class LocaleParserInfo(parserinfo):
        WEEKDAYS = zip(calendar.day_abbr, calendar.day_name)
        MONTHS = list(zip(calendar.month_abbr, calendar.month_name))[1:]

    parsed_date = parse(date, parserinfo=LocaleParserInfo())
    if old_locale:
        locale.setlocale(locale.LC_ALL, old_locale)
    else:
        locale.setlocale(locale.LC_ALL, '')
    return parsed_date


def get_saved_url(fname):
    url = None
    saved_url = Path(fname)
    if saved_url.exists():
        with saved_url.open(encoding='utf-8') as f:
            data = yaml.safe_load(f)
            if data:
                url = data.get('URL')
    return url


def save_url(fname, url):
    if url:
        saved_url = Path(fname)
        data = {'URL': url}
        with saved_url.open('w', encoding='utf-8') as f:
            yaml.dump(data, f)
