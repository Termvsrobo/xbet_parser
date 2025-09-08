import calendar
import locale

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
