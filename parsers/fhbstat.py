import json
import re
from asyncio import sleep
from collections import defaultdict
from contextlib import asynccontextmanager
from copy import copy
from datetime import datetime
from decimal import ROUND_DOWN, Decimal
from enum import IntEnum
from functools import reduce
from itertools import count
from pathlib import Path
from random import randint
from typing import Annotated, Dict, List, Literal, Optional, Union
from urllib.parse import parse_qs, unquote, urlencode, urlparse, urlunparse

import httpx
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from nicegui.events import UploadEventArguments
from openpyxl.styles import Border, Side
from openpyxl.worksheet.cell_range import CellRange
from pydantic import (BaseModel, Discriminator, Field, PositiveInt, RootModel,
                      Tag, TypeAdapter)
from xlsxtpl.writerx import BookWriter

from base import Parser
from config import settings


class FieldType(IntEnum):
    BOOL: int = 1
    TIME: int = 2
    FLOAT: int = 3
    STR: int = 4


def filter_type_discriminator(v):
    result = None
    if isinstance(v, dict):
        result = v.get('type', None)
    else:
        result = getattr(v, 'type', None)
    return result


class BaseFilterField(BaseModel):
    filter_value: str
    column: int
    priority: Optional[PositiveInt] = None

    def get_value(self, value, filter_value: Optional[str] = None):
        return value

    def next_value(self, value):
        _filter_value = self.filter_value
        for _ in range(2):
            yield self.get_value(value, _filter_value)
            _filter_value = _filter_value[:-1]

    class Config:
        validate_assignment = True


class FloatField(BaseFilterField):
    type: Literal[FieldType.FLOAT]
    filter_value: Optional[str] = '0.1'

    def get_value(self, value, filter_value: Optional[str] = None):
        _filter_value = filter_value or self.filter_value
        exp = Decimal(_filter_value).as_tuple().exponent * -1
        adjust_value = 10 ** (-1 * (exp + 2))
        _value = Decimal(value + adjust_value).quantize(Decimal(_filter_value), rounding=ROUND_DOWN)
        _value = float(_value)
        if _value.is_integer() and re.match(r'^\d+.$', _filter_value):
            _value = str(int(_value)) + '.'
        elif _value.is_integer() and re.match(r'^\d+$', _filter_value):
            _value = str(int(_value))
        elif _value.is_integer() and re.match(r'^\d+.\d+$', _filter_value):
            _value = str(int(_value)) + '.0'
        else:
            _value = str(_value)
        return _value


class TimeField(BaseFilterField):
    type: Literal[FieldType.TIME]
    filter_value: Optional[str] = '00:00'

    def get_value(self, value, filter_value: Optional[str] = None):
        _filter_value = filter_value or self.filter_value
        result = ''
        if ':' not in _filter_value:
            result = value.split(':')[0]
        elif _filter_value.endswith(':'):
            result = value.split(':')[0] + ':'
        else:
            result = value
        return result


class StrField(BaseFilterField):
    type: Literal[FieldType.STR]


class BoolField(BaseFilterField):
    type: Literal[FieldType.BOOL]
    filter_value: bool = True


TypedField = Annotated[
    Union[
        Annotated[BoolField, Tag(FieldType.BOOL)],
        Annotated[StrField, Tag(FieldType.STR)],
        Annotated[FloatField, Tag(FieldType.FLOAT)],
        Annotated[TimeField, Tag(FieldType.TIME)]
    ],
    Discriminator(filter_type_discriminator)
]

ta = TypeAdapter(TypedField)


class FHBStatFilter(BaseModel):
    filter_id: PositiveInt
    filters: List[TypedField]


class Filters(RootModel):
    root: Optional[List[FHBStatFilter]] = Field(default_factory=list)


class FHBParser(Parser):
    count_columns: int = 256
    max_time_sleep_sec: int = 1
    round_precision: str = '0.1'
    datetime_round: str = '00:00'
    count_empty_rows: int = 4
    digits_columns_start: int = 25

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._user_agent = 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36'  # noqa:E501
        self._email = None
        self._password = None
        self._url = 'https://fhbstat.com'
        self.target_urls: Optional[defaultdict] = defaultdict(str)
        self.file_name: str = ''
        self.from_time: str = ''
        self.to_time: str = ''
        self.user_filters: Optional[Filters] = Filters()

    @property
    def email(self):
        email = None
        if self._email:
            email = self._email
        return email

    @email.setter
    def email(self, value):
        if value:
            self._email = value

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value):
        if value:
            self._password = value

    def stop(self):
        super().stop()
        self._email = None
        self._password = None

    def parser_log_filter(self, record):
        return __name__ == record['name']

    @property
    def columns(self):
        return list(range(1, self.count_columns))

    def get_filter_id(self):
        result = 1
        if self.user_filters.root:
            last_id = max(map(lambda x: x.filter_id, self.user_filters.root))
            result = last_id + 1
        return result

    def add_user_filter(self, column, filter_value=None, priority=None, filter_id=None):
        exist_filter = next(
            filter(lambda x: getattr(x, 'filter_id') == filter_id, self.user_filters.root),
            None
        )
        filter_data_dict = dict(
            column=column,
            priority=priority,
            type=self.get_field_type(column)
        )
        if filter_value:
            filter_data_dict['filter_value'] = filter_value
        if not exist_filter:
            self.user_filters.root.append(
                FHBStatFilter(
                    filter_id=filter_id or self.get_filter_id(),
                    filters=[
                        ta.validate_python(
                            filter_data_dict
                        )
                    ]
                )
            )
        else:
            exist_column_filter = next(
                filter(lambda x: x.column == column, exist_filter.filters),
                None
            )
            if exist_column_filter:
                if filter_value:
                    exist_column_filter.filter_value = filter_value
                if priority:
                    exist_column_filter.priority = priority
            else:
                exist_filter.filters.append(
                    ta.validate_python(
                        filter_data_dict
                    )
                )

    def remove_user_filter(self, filter_id, column):
        for _filter in self.user_filters.root:
            if _filter.filter_id == filter_id:
                for _filter_ in _filter.filters:
                    if _filter_.column == column:
                        _filter.filters.remove(_filter_)
                        break

    def get_used_columns_by_filter(self, filter_id):
        result = []
        for _filter in self.user_filters.root:
            if _filter.filter_id == filter_id:
                result = [_filter_.column for _filter_ in _filter.filters]
        return result

    def download_filters(self):
        return JSONResponse(self.user_filters.model_dump())

    def upload_filters(self, upload_file: UploadEventArguments):
        data = json.load(upload_file.content)
        self.user_filters = self.user_filters.model_validate(data)

    def upload_filters_from_json(self, json_file: Path):
        self.user_filters = self.user_filters.model_validate_json(json_file.read_bytes())

    async def login(self, client: httpx.AsyncClient):
        self.status = 'Логинимся'
        cookies_file = Path('cookies.json')
        cookies = {}
        if cookies_file.exists():
            with cookies_file.open() as f:
                cookies = json.load(f)
        response = await client.post(
            'https://fhbstat.com/авторизация',
            data={
                'posts[className]': 'вход',
                'posts[value][email]': self.email,
                'posts[value][пароль]': self.password,
                'posts[location]': 'https://fhbstat.com/авторизация',
            },
            cookies=cookies,
        )
        assert response.status_code == 200, 'Не удалось авторизоваться на сайте fhbstat.com'
        try:
            json_data = response.json()
        except Exception:
            self.logger.exception('Ошибка во время авторизации')
        else:
            if 'success' in json_data and 'error' in json_data['success']:
                self.status = json_data['success']['error']
                return False
            else:
                with cookies_file.open('w') as f:
                    json.dump(dict(client.cookies), f)
        return True

    async def logout(self, client: httpx.AsyncClient):
        self.status = 'Выходим'
        response = await client.post(
            'https://fhbstat.com/авторизация',
            data={
                'posts[className]': 'выход',
                'posts[value]': '',
                'posts[location]': 'https://fhbstat.com/авторизация',
            }
        )
        assert response.status_code == 200, 'Не удалось авторизоваться на сайте fhbstat.com'
        self.status = 'Вышли'

    @asynccontextmanager
    async def page_client(self, client: httpx.AsyncClient):
        try:
            is_logged = await self.login(client=client)
            if is_logged:
                yield client
            else:
                yield None
        except Exception:
            self.logger.exception('Ошибка')
        finally:
            await self.logout(client=client)

    @classmethod
    def get_excel_template(cls, path):
        templates = {
            '/football': 'ШАБЛОН Эксель Футбол Исход.xlsx',
            '/football_total': 'ШАБЛОН Эксель Футбол Тотал.xlsx',
            '/football_24': 'ШАБЛОН Эксель Футбол 24.xlsx',
            '/hockey': 'ШАБЛОН Эксель Хоккей Исход.xlsx',
            '/hockey_total': 'ШАБЛОН Эксель Хоккей Тотал.xlsx',
            '/hockey_24': 'ШАБЛОН Эксель Хоккей 24.xlsx',
        }
        return templates.get(path)

    def get_file_response(self, df_data, target_path):
        result = None
        if df_data:
            msg = f'Собрано данных: {len(df_data)}'
            self.status = msg
            df = pd.DataFrame.from_records(df_data)
            df['Дата слепка, МСК'] = self.now_msk
            columns = list(
                map(str, self.columns)
            ) + ['index', 'dt', 'Количество матчей', 'Дата слепка, МСК', 'url']
            df = df.reindex(columns=columns)
            df['Дата слепка, МСК'] = df['Дата слепка, МСК'].dt.tz_localize(None)
            older_df = pd.DataFrame(columns=columns)
            if self.file_name:
                self.path = f'files/{self.file_name}.xlsx'
                filename = f'{self.file_name}.xlsx'
            else:
                self.path = f'files/{self.name}_{self.now_msk.isoformat()}.xlsx'
                filename = f'{self.name}_{self.now_msk.isoformat()}.xlsx'
            if older_df.empty:
                full_df = df
            else:
                full_df = pd.concat((df, older_df))
            if settings.DEBUG:
                full_df.to_excel('files/debug.xlsx', index=False, columns=columns)
            full_df = full_df.reset_index(drop=True)

            template_name = self.get_excel_template(target_path)
            if template_name:
                fname = Path(__file__).parent.parent / Path('excel_templates') / Path(template_name)
                writer = BookWriter(fname)
                writer.jinja_env.globals.update(dir=dir, getattr=getattr)

                data = dict()
                data['rows'] = df.to_dict('records')
                payload0 = {'tpl_idx': 1, 'sheet_name': 'Статистика',  'ctx': data}

                payloads = [payload0]
                writer.render_book2(payloads=payloads)

                workbook = writer.workbook
                sheet = workbook.active
                start_row = None
                start_column = None
                split_column = None
                link_column = None
                for i, value in enumerate(sheet.values):
                    if 'Ссылка' in value:
                        link_column = value.index('Ссылка') + 1
                    if '№' in value and 'Количество матчей' in value:
                        start_row = i + 4
                        start_column = value.index('№') + 1
                        split_column = value.index('Количество матчей') + 1
                        break

                max_rows = start_row
                for row in range(start_row + 1, sheet.max_row + 1):
                    if sheet.cell(row=row, column=start_column).value is None:
                        max_rows = row
                        break

                for col in range(start_column, split_column):
                    first_row = start_row
                    end_row = first_row + len(self.user_filters.root) + 3 + self.count_empty_rows - 1
                    while end_row <= max_rows:
                        if col == start_column:
                            sheet.merge_cells(
                                start_column=col,
                                end_column=col,
                                start_row=first_row,
                                end_row=end_row
                            )
                            max_column = sheet.max_column
                            if link_column:
                                max_column -= 1
                            cell_range = CellRange(
                                min_col=col,
                                max_col=max_column,
                                min_row=first_row,
                                max_row=end_row
                            )
                            sides = ('left', 'right', 'top', 'bottom')
                            for side in sides:
                                for cell in getattr(cell_range, side, []):
                                    _cell = sheet.cell(cell[0], cell[1])
                                    other_sides = filter(lambda _side: _side != side, sides)
                                    old_border = copy(_cell.border)
                                    _cell.border = Border(
                                        **{side: Side(border_style='thick')},
                                        **{
                                            other_side: getattr(old_border, other_side)
                                            for other_side in other_sides
                                        }
                                    )
                        else:
                            sheet.merge_cells(
                                start_column=col,
                                end_column=col,
                                start_row=first_row,
                                end_row=first_row + len(self.user_filters.root) - 1
                            )
                        first_row = end_row + 1
                        end_row = first_row + len(self.user_filters.root) + 3 + self.count_empty_rows - 1

                if link_column:
                    for row in range(start_row, max_rows + 1):
                        value = sheet.cell(row, link_column).value
                        if value and isinstance(value, str):
                            sheet.cell(row, link_column).hyperlink = value
                            sheet.cell(row, link_column).style = "Hyperlink"

                writer.save(self.path)

                result = FileResponse(
                    self.path,
                    filename=filename
                )
            else:
                result = PlainTextResponse('Не нашли шаблон excel.')
        else:
            result = PlainTextResponse('Не собрали данных.')
        return result

    @classmethod
    def get_head_data(cls, content):
        first_data_index = None
        names = []
        soup = BeautifulSoup(content, 'lxml')
        table_rows = list(filter(lambda tr: tr != '\n', soup.table.tbody.contents))
        first_data_row = next(
            filter(lambda tr: 'data-status' in tr.attrs, table_rows),
            None
        )
        if first_data_row:
            first_data_index = table_rows.index(first_data_row)
            names = list(
                map(
                    lambda x: x.text,
                    filter(lambda td: td != '\n' and td.text != '', table_rows[first_data_index - 1].contents)
                )
            )
        return table_rows, first_data_index, names

    @classmethod
    def parse_head_table(cls, content):
        table_rows, first_data_index, names = cls.get_head_data(content)
        if first_data_index:
            data_rows = table_rows[3:4]
            data_list = list()
            key_name = 'data-formula'
            for data in data_rows:
                data_row = dict()
                for td in data.contents:
                    if td != '\n':
                        if key_name in td.attrs:
                            key = td.attrs.get(key_name)
                            value = td.text
                            data_row[key] = float(value) if value else np.nan
                if data_row:
                    data_list.append(data_row)
            df = pd.DataFrame.from_records(data_list, columns=names + ['dt'])
            df = df.replace({None: np.nan, '': np.nan})
        else:
            df = pd.DataFrame()
        return df

    @classmethod
    def parse_content(cls, content):
        table_rows, first_data_index, names = cls.get_head_data(content)
        if first_data_index:
            data_rows = table_rows[first_data_index:]
            data_list = list()
            key_name = 'data-td'
            for data in data_rows:
                data_row = dict()
                for td in data.contents:
                    if td != '\n':
                        if key_name in td.attrs:
                            key = td.attrs.get(key_name)
                            value = td.text
                            if value:
                                try:
                                    if value.isnumeric():
                                        data_row[key] = int(value)
                                    else:
                                        data_row[key] = float(value)
                                except ValueError:
                                    data_row[key] = value
                            else:
                                data_row[key] = np.nan
                _dt_str = f'{data_row.get("3")}-{data_row.get("2")}-{data_row.get("1")} {data_row.get("4")}'
                try:
                    _dt = datetime.strptime(_dt_str, '%Y-%m-%d %H:%M')
                except ValueError:
                    continue
                else:
                    data_row['dt'] = _dt
                    data_list.append(data_row)
            df = pd.DataFrame.from_records(data_list, columns=names + ['dt'])
            df = df.replace({None: np.nan, '': np.nan})
        else:
            df = pd.DataFrame()
        return df

    def get_field_type(self, value):
        if value == 4:
            return FieldType.TIME
        elif value < 11:
            return FieldType.BOOL
        elif value >= 11:
            return FieldType.FLOAT
        else:
            return FieldType.STR

    @classmethod
    def get_means(cls, list_values: List[Dict[str, float]]):
        result = dict()
        if list_values:
            keys = set(reduce(lambda x, y: x + y, [list(lv.keys()) for lv in list_values]))
        else:
            keys = set()
        keys = set(
            filter(
                lambda x: (
                    x not in ('index', 'dt', 'url')
                    and (x == 'Количество матчей' or int(x) >= cls.digits_columns_start)
                ),
                keys
            )
        )
        res = {key: np.array([d.get(key, np.nan) for d in list_values]) for key in keys}
        for k, v in res.items():
            res[k] = v.astype('float')
        if 'Количество матчей' in res:
            count_matches = res.pop('Количество матчей')
            for key, value in res.items():
                if np.nansum(count_matches) > 0:
                    result[key] = np.nansum(value * count_matches) / np.nansum(count_matches)
                    result[key] = result[key].round(4)
                else:
                    result[key] = np.nan
        else:
            for key, value in res.items():
                result[key] = np.nan
        return result

    @classmethod
    def get_mathematical_expectation(cls, data_means, data_match):
        keys = set(list(data_means.keys()) + list(data_match.keys()))
        result = dict()
        for key in keys:
            key_mean = data_means.get(key, 0)
            key_match = data_match.get(key, 0)
            if key_mean and key_match:
                result[key] = (key_mean / 100 * key_match) - 1
        return result

    @classmethod
    def filter_df_by_time(cls, df: pd.DataFrame, from_time: str, to_time: str) -> pd.DataFrame:
        _df = df
        if not df.empty:
            if all([from_time, to_time]):
                _df = df.set_index('dt')
                _df = _df.between_time(from_time, to_time)
                _df = _df.reset_index()
            elif from_time:
                _df = df[df['dt'].dt.time >= datetime.strptime(from_time, '%H:%M').time()]
            elif to_time:
                _df = df[df['dt'].dt.time <= datetime.strptime(to_time, '%H:%M').time()]
        return _df

    def get_url_params(self, url):
        scheme, domain, path, params, query, fragment = urlparse(url)
        query_params = parse_qs(query)
        target_url = urlunparse((scheme, domain, path, params, None, fragment))
        return target_url, query_params, path

    async def parse(self, browser):
        result = None
        msg = f'Открываем {self.url}'
        self.status = msg

        async with httpx.AsyncClient(
            follow_redirects=True,
            headers={
                'User-Agent': self._user_agent
            }
        ) as client:
            async with self.page_client(client=client) as logged_client:
                if logged_client is not None:
                    dfs = []
                    result_df_list = []
                    copy_target_urls = self.target_urls.copy()
                    for target_url in copy_target_urls.values():
                        self.status = f'Обрабатываем ссылку {target_url}'
                        _target_url, query_params, target_path = self.get_url_params(target_url)
                        for key, value in query_params.items():
                            if isinstance(value, (list, tuple)) and len(value) == 1:
                                query_params[key] = value[0]
                        if 'page' not in query_params:
                            for page_number in count(1):
                                if page_number == 1:
                                    response = await logged_client.get(
                                        _target_url,
                                        params=query_params
                                    )
                                else:
                                    response = await logged_client.get(
                                        _target_url,
                                        params={'page': page_number, **query_params}
                                    )
                                if response.status_code == 200:
                                    try:
                                        df = self.parse_content(response.content)
                                        df = self.filter_df_by_time(df, self.from_time, self.to_time)
                                    except Exception:
                                        self.logger.exception('Ошибка сбора данных. Возможно не оплачен тариф.')
                                        self.status = 'Ошибка сбора данных. Возможно не оплачен тариф.'
                                        break
                                    else:
                                        if not df.empty:
                                            dfs.append(df)
                                        else:
                                            break
                                await sleep(randint(1, self.max_time_sleep_sec))
                        else:
                            response = await logged_client.get(
                                _target_url,
                                params=query_params
                            )
                            if response.status_code == 200:
                                try:
                                    df = self.parse_content(response.content)
                                    df = self.filter_df_by_time(df, self.from_time, self.to_time)
                                except Exception:
                                    self.logger.exception('Ошибка сбора данных. Возможно не оплачен тариф.')
                                    self.status = 'Ошибка сбора данных. Возможно не оплачен тариф.'
                                else:
                                    if not df.empty:
                                        dfs.append(df)
                        future_data = pd.DataFrame()
                        if dfs:
                            future_data = pd.concat(dfs)
                        data_records = future_data.to_dict(orient='records')
                        self.count_links = len(data_records)
                        for index, data_match in enumerate(self.tqdm(data_records), 1):
                            local_match_result_df = []
                            for user_filter in self.user_filters.root:
                                filters_data = {}
                                for _filter in user_filter.filters:
                                    value_match = data_match.get(str(_filter.column))
                                    filters_data[str(_filter.column)] = _filter.get_value(value_match)
                                scheme, domain, path, params, _, fragment = urlparse(_target_url)
                                priority_queues = sorted(
                                    filter(
                                        lambda x: x.priority is not None,
                                        user_filter.filters
                                    ),
                                    key=lambda x: x.priority
                                )
                                if priority_queues:
                                    _filters_data = filters_data.copy()
                                    for priority_filter in priority_queues:
                                        value_match = data_match.get(str(priority_filter.column))
                                        data_exist = False
                                        for next_value in priority_filter.next_value(value_match):
                                            _filters_data[str(priority_filter.column)] = next_value
                                            page_url = urlunparse((
                                                scheme, domain, path, params, urlencode(_filters_data), fragment
                                            ))
                                            cookies = [
                                                {
                                                    'name': key,
                                                    'value': value,
                                                    'domain': 'fhbstat.com',
                                                    'path': '/'
                                                }
                                                for key, value in logged_client.cookies.items()
                                            ]
                                            await browser.add_cookies(cookies)
                                            page = await browser.new_page()
                                            await page.set_extra_http_headers({
                                                "User-Agent": self._user_agent
                                            })
                                            await page.goto(page_url)
                                            await page.wait_for_load_state()
                                            page_content = await page.content()
                                            df_match = self.parse_content(page_content)
                                            if not df_match.empty:
                                                df_match = df_match.loc[
                                                    df_match['dt'].dt.tz_localize('Europe/Moscow') <= self.now_msk
                                                ]
                                            head_df = self.parse_head_table(page_content)
                                            await page.close()
                                            columns = list(
                                                filter(
                                                    lambda x: int(x) >= self.digits_columns_start,
                                                    head_df.columns[:-1]
                                                )
                                            )
                                            head_df_records = head_df.to_dict(orient='records')
                                            copy_data_match = data_match.copy()
                                            for h_d_r in head_df_records:
                                                for column_name, column_value in h_d_r.items():
                                                    if column_name in columns:
                                                        copy_data_match[column_name] = column_value
                                            count_rows, _ = df_match.shape
                                            copy_data_match['Количество матчей'] = count_rows
                                            copy_data_match['index'] = index
                                            copy_data_match['url'] = unquote(page_url)
                                            if count_rows:
                                                local_match_result_df.append(copy_data_match)
                                                data_exist = True
                                                break
                                            else:
                                                await sleep(randint(1, self.max_time_sleep_sec))
                                        if data_exist:
                                            break
                                    if not data_exist:
                                        local_match_result_df.append(
                                            {
                                                **{str(i): np.nan for i in self.columns},
                                                **{
                                                    'index': index,
                                                    'Количество матчей': 0,
                                                    'url': unquote(
                                                        urlunparse((
                                                            scheme,
                                                            domain,
                                                            path,
                                                            params,
                                                            urlencode(filters_data),
                                                            fragment
                                                        ))
                                                    )
                                                }
                                            }
                                        )
                                else:
                                    page_url = urlunparse((
                                        scheme, domain, path, params, urlencode(filters_data), fragment
                                    ))
                                    cookies = [
                                        {
                                            'name': key,
                                            'value': value,
                                            'domain': 'fhbstat.com',
                                            'path': '/'
                                        }
                                        for key, value in logged_client.cookies.items()
                                    ]
                                    await browser.add_cookies(cookies)
                                    page = await browser.new_page()
                                    await page.set_extra_http_headers({
                                        "User-Agent": self._user_agent
                                    })
                                    await page.goto(page_url)
                                    await page.wait_for_load_state()
                                    page_content = await page.content()
                                    df_match = self.parse_content(page_content)
                                    if not df_match.empty:
                                        df_match = df_match.loc[
                                            df_match['dt'].dt.tz_localize('Europe/Moscow') <= self.now_msk
                                        ]
                                    head_df = self.parse_head_table(page_content)
                                    await page.close()
                                    columns = list(
                                        filter(
                                            lambda x: int(x) >= self.digits_columns_start,
                                            head_df.columns[:-1]
                                        )
                                    )
                                    head_df_records = head_df.to_dict(orient='records')
                                    copy_data_match = data_match.copy()
                                    for h_d_r in head_df_records:
                                        for column_name, column_value in h_d_r.items():
                                            if column_name in columns:
                                                copy_data_match[column_name] = column_value
                                    count_rows, _ = df_match.shape
                                    copy_data_match['Количество матчей'] = count_rows
                                    copy_data_match['index'] = index
                                    copy_data_match['url'] = unquote(page_url)
                                    local_match_result_df.append(copy_data_match)
                                await sleep(randint(1, self.max_time_sleep_sec))
                            result_df_list += local_match_result_df
                            means = self.get_means(local_match_result_df)
                            mathematical_expectation = self.get_mathematical_expectation(means, data_match)
                            result_df_list.append({
                                **means,
                                **{
                                    'index': index,
                                    'Количество матчей': '%'
                                }
                            })
                            result_df_list.append({
                                **{
                                    str(i): data_match.get(str(i))
                                    for i in self.columns if i >= self.digits_columns_start
                                },
                                **{
                                    'index': index,
                                    'Количество матчей': 'кф'
                                }
                            })
                            result_df_list.append({
                                **mathematical_expectation,
                                **{
                                    'index': index,
                                    'Количество матчей': 'мо'
                                }
                            })
                            # Добавляем пустые строки
                            for _ in range(self.count_empty_rows):
                                result_df_list.append({
                                    **{str(i): np.nan for i in self.columns},
                                    **{'index': index}
                                })
                    result = self.get_file_response(df_data=result_df_list, target_path=target_path)
                    return result
