import json
import re
from asyncio import sleep
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import ROUND_DOWN, Decimal
from functools import reduce
from itertools import count
from pathlib import Path
from random import randint
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from fastapi.responses import FileResponse, PlainTextResponse
from xlsxtpl.writerx import BookWriter

try:
    from pathvalidate import sanitize_filename
except ImportError:
    sanitize_filename = None

from base import Parser
from config import settings


class FHBParser(Parser):
    count_columns: int = 256

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._user_agent = 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36'  # noqa:E501
        self._email = None
        self._password = None
        self._url = 'https://fhbstat.com/football'
        self._filters = dict()
        self.rounded_fields = defaultdict(dict)
        self.target_urls: Optional[set] = set()

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
        self._filters.clear()
        self.rounded_fields.clear()
        self.target_urls.clear()

    def parser_log_filter(self, record):
        return __name__ == record['name']

    def add_filter(self, element):
        self._filters[str(element.sender.label)] = element.value

    @property
    def filters(self):
        return self._filters

    async def login(self, client: httpx.AsyncClient):
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
        response = await client.post(
            'https://fhbstat.com/авторизация',
            data={
                'posts[className]': 'выход',
                'posts[value]': '',
                'posts[location]': 'https://fhbstat.com/авторизация',
            }
        )
        assert response.status_code == 200, 'Не удалось авторизоваться на сайте fhbstat.com'

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
        }
        return templates.get(path)

    def get_file_response(self, df_data, target_path):
        result = None
        if df_data:
            msg = f'Собрано данных: {len(df_data)}'
            self.logger.info(msg)
            self.status = msg
            df = pd.DataFrame.from_records(df_data)
            df['Дата слепка, МСК'] = self.now_msk
            columns = list(
                map(str, range(1, self.count_columns))
            ) + ['index', 'dt', 'Количество матчей', 'Дата слепка, МСК']
            df = df.reindex(columns=columns)
            df['Дата слепка, МСК'] = df['Дата слепка, МСК'].dt.tz_localize(None)
            older_df = pd.DataFrame(columns=columns)
            self.path = f'files/{self.name}_{self.now_msk.isoformat()}.xlsx'
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
                writer.save(self.path)
                result = FileResponse(
                    self.path,
                    filename=f'{self.name}_{self.now_msk.isoformat()}.xlsx'
                )
            else:
                result = PlainTextResponse('Не нашли шаблон excel.')
        else:
            result = PlainTextResponse('Не собрали данных.')
        return result

    @classmethod
    def parse_head_table(cls, content):
        soup = BeautifulSoup(content, 'lxml')
        table_rows = list(filter(lambda tr: tr != '\n', soup.table.tbody.contents))
        names = list(map(lambda x: x.text, filter(lambda td: td != '\n' and td.text != '', table_rows[13].contents)))
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
            data_list.append(data_row)
        df = pd.DataFrame.from_records(data_list, columns=names + ['dt'])
        df = df.replace({None: np.nan, '': np.nan})
        return df

    @classmethod
    def parse_content(cls, content):
        soup = BeautifulSoup(content, 'lxml')
        table_rows = list(filter(lambda tr: tr != '\n', soup.table.tbody.contents))
        names = list(map(lambda x: x.text, filter(lambda td: td != '\n' and td.text != '', table_rows[13].contents)))
        data_rows = table_rows[14:]
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
        return df

    def get_field_type(self, value):
        if value < 11:
            return bool
        elif value >= 11:
            return float
        else:
            return str

    @classmethod
    def round(cls, value, precision: str = '0'):
        _value = Decimal(value).quantize(Decimal(precision), rounding=ROUND_DOWN)
        _value = float(_value)
        if _value.is_integer() and re.match(r'^\d+.$', precision):
            _value = str(int(_value)) + '.'
        elif _value.is_integer() and re.match(r'^\d+$', precision):
            _value = str(int(_value))
        elif _value.is_integer() and re.match(r'^\d+.\d+$', precision):
            _value = str(int(_value)) + '.0'
        else:
            _value = str(_value)
        return _value

    @classmethod
    def get_means(cls, list_values: List[Dict[str, float]]):
        keys = set(reduce(lambda x, y: x + y, [list(lv.keys()) for lv in list_values]))
        keys = set(filter(lambda x: x not in ('index', 'Количество матчей', 'dt') and int(x) >= 11, keys))
        res = {key: np.array([d.get(key, np.nan) for d in list_values]) for key in keys}
        for k, v in res.items():
            res[k] = v.astype('float')
        result = dict()
        for key, value in res.items():
            result[key] = value[~np.isnan(value)].mean()
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

    def get_url_params(self, url):
        scheme, domain, path, params, query, fragment = urlparse(url)
        query_params = parse_qs(query)
        target_url = urlunparse((scheme, domain, path, params, None, fragment))
        return target_url, query_params, path

    async def parse(self, browser):
        result = None
        msg = f'Открываем {self.url}'
        self.logger.info(msg)
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
                    for target_url in self.target_urls:
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
                                    except Exception:
                                        self.logger.exception('Ошибка сбора данных. Возможно не оплачен тариф.')
                                        self.status = 'Ошибка сбора данных. Возможно не оплачен тариф.'
                                        break
                                    else:
                                        if not df.empty:
                                            dfs.append(df)
                                        else:
                                            break
                                await sleep(randint(1, 3))
                        else:
                            response = await logged_client.get(
                                _target_url,
                                params=query_params
                            )
                            if response.status_code == 200:
                                try:
                                    df = self.parse_content(response.content)
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
                            _rounded_fields = self.rounded_fields.copy()
                            local_match_result_df = []
                            for filters_value in _rounded_fields.values():
                                filters_data = {}
                                for i, data in filters_value.items():
                                    field_type = self.get_field_type(i)
                                    _data_match = data_match.get(str(i))
                                    if _data_match:
                                        if issubclass(field_type, bool):
                                            filters_data[str(i)] = _data_match
                                        else:
                                            filters_data[str(i)] = self.round(_data_match, str(data))
                                scheme, domain, path, params, _, fragment = urlparse(_target_url)
                                page_url = urlunparse((scheme, domain, path, params, urlencode(filters_data), fragment))
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
                                if sanitize_filename:
                                    parent_dir = Path('html') / Path(self.now_msk.isoformat())
                                    parent_dir.mkdir(parents=True, exist_ok=True)
                                    fname = parent_dir / Path(
                                        f'{sanitize_filename(page_url.replace("https://fhbstat.com/", ""))}.html'
                                    )
                                    fname.write_text(page_content)
                                await page.close()
                                columns = list(filter(lambda x: int(x) >= 25, head_df.columns[:-1]))
                                head_df_records = head_df.to_dict(orient='records')
                                _data_match = data_match.copy()
                                for h_d_r in head_df_records:
                                    for column_name, column_value in h_d_r.items():
                                        if column_name in columns:
                                            _data_match[column_name] = column_value
                                count_rows, _ = df_match.shape
                                _data_match['Количество матчей'] = count_rows
                                _data_match['index'] = index
                                local_match_result_df.append(_data_match)
                                await sleep(randint(1, 3))
                            result_df_list += local_match_result_df
                            means = self.get_means(local_match_result_df)
                            mathematical_expectation = self.get_mathematical_expectation(means, data_match)
                            result_df_list.append({
                                **means,
                                **{'Количество матчей': '%'}
                            })
                            result_df_list.append({
                                **{str(i): data_match.get(str(i)) for i in range(1, self.count_columns) if i >= 25},
                                **{'Количество матчей': 'кф'}
                            })
                            result_df_list.append({
                                **mathematical_expectation,
                                **{'Количество матчей': 'мо'}
                            })
                            result_df_list.append({str(i): None for i in range(1, self.count_columns)})
                    result = self.get_file_response(df_data=result_df_list, target_path=target_path)
                    return result
