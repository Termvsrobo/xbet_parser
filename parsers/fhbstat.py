import json
from contextlib import asynccontextmanager
from datetime import datetime
from itertools import count
from pathlib import Path

import httpx
import pandas as pd
import pytz
from bs4 import BeautifulSoup
from fastapi.responses import FileResponse, PlainTextResponse

from base import Parser
from config import settings


class FHBParser(Parser):
    def parser_log_filter(self, record):
        return __name__ == record['name']

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
                'posts[value][email]': 'termvsrobo@yandex.ru',
                'posts[value][пароль]': '272000359',
                'posts[location]': 'https://fhbstat.com/авторизация',
            },
            cookies=cookies,
        )
        assert response.status_code == 200, 'Не удалось авторизоваться на сайте fhbstat.com'
        with cookies_file.open('w') as f:
            json.dump(dict(client.cookies), f)
        try:
            json_data = response.json()
        except Exception:
            pass
        else:
            if 'success' in json_data and 'error' in json_data['success']:
                self.status = json_data['success']['error']
                return False
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

    def get_file_response(self, df_data):
        result = None
        if df_data:
            msg = f'Собрано данных: {len(df_data)}'
            self.logger.info(msg)
            self.status = msg
            df = pd.DataFrame.from_records(df_data)
            now_msk = datetime.now(tz=pytz.timezone('Europe/Moscow'))
            df['Дата слепка, МСК'] = now_msk
            columns = [
                '1',
                '2',
                '3',
                '4',
                '5',
                '6',
                '7',
                '8',
                '9',
                '10',
                '11',
                '12',
                '13',
                '14',
                '15',
                '16',
                '18',
                '19',
                '20',
                '21',
                '22',
                '23',
                '25',
                '26',
                '27',
                '32',
                '33',
                '34',
                '28',
                '29',
                '30',
                '35',
                '36',
                '37',
                '38',
                '39',
                '127',
                '113',
                '114',
                '128',
                '92',
                '95',
                '129',
                '44',
                '45',
                '46',
                '130',
                '47',
                '48',
                '49',
                '131',
                '50',
                '51',
                '52',
                '53',
                '54',
                '55',
                '56',
                '57',
                '58',
                'dt',
                'Количество матчей',
                'Дата слепка, МСК'

            ]
            df = df.reindex(columns=columns)
            df['Дата слепка, МСК'] = df['Дата слепка, МСК'].dt.tz_localize(None)
            older_df = pd.DataFrame(columns=columns)
            self.path = f'files/{self.name}_{now_msk.isoformat()}.xlsx'
            if older_df.empty:
                full_df = df
            else:
                full_df = pd.concat((df, older_df))
            if settings.DEBUG:
                full_df.to_excel('files/debug.xlsx', index=False, columns=columns)
            full_df = full_df.sort_values(
                [
                    'dt',
                    '9',
                    '10',
                ],
                ascending=[False, True, True]
            )
            full_df = full_df.reset_index(drop=True)

            with pd.ExcelWriter(self.path, datetime_format='%d.%m.%y %H:%M') as writer:
                full_df.to_excel(writer, index=False, startrow=1, columns=columns)
            result = FileResponse(
                self.path,
                filename=f'{self.name}_{now_msk.isoformat()}.xlsx'
            )
        else:
            result = PlainTextResponse('Не собрали данных')
        return result

    @classmethod
    def parse_head_table(cls, content):
        soup = BeautifulSoup(content, 'lxml')
        table_rows = list(filter(lambda tr: tr != '\n', soup.table.tbody.contents))
        names = list(map(lambda x: x.text, filter(lambda td: td != '\n' and td.text != '', table_rows[13].contents)))
        data_rows = table_rows[:14]
        data_list = list()
        for data in data_rows:
            data_row = dict()
            for td in data.contents:
                if td != '\n':
                    if 'data-summa' in td.attrs:
                        key = td.attrs.get('data-td')
                        value = td.text
                        data_row[key] = value
            data_list.append(data_row)
        df = pd.DataFrame.from_records(data_list, columns=names + ['dt'])
        return df

    @classmethod
    def parse_content(cls, content):
        soup = BeautifulSoup(content, 'lxml')
        table_rows = list(filter(lambda tr: tr != '\n', soup.table.tbody.contents))
        names = list(map(lambda x: x.text, filter(lambda td: td != '\n' and td.text != '', table_rows[13].contents)))
        data_rows = table_rows[14:]
        data_list = list()
        for data in data_rows:
            data_row = dict()
            for td in data.contents:
                if td != '\n':
                    if 'data-td' in td.attrs:
                        key = td.attrs.get('data-td')
                        value = td.text
                        data_row[key] = value
            _dt_str = f'{data_row.get("3")}-{data_row.get("2")}-{data_row.get("1")} {data_row.get("4")}'
            _dt = datetime.strptime(_dt_str, '%Y-%m-%d %H:%M')
            data_row['dt'] = _dt
            data_list.append(data_row)
        df = pd.DataFrame.from_records(data_list, columns=names + ['dt'])
        return df

    async def parse(self, browser):
        await browser.close()
        result = None
        msg = f'Открываем {self.url}'
        self.logger.info(msg)
        self.status = msg

        async with httpx.AsyncClient(
            follow_redirects=True,
            headers={
                'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36'  # noqa:E501
            }
        ) as client:
            async with self.page_client(client=client) as logged_client:
                if logged_client is not None:
                    dfs = []
                    for page_number in count(1):
                        if page_number == 1:
                            response = await logged_client.get(
                                'https://fhbstat.com/football',
                            )
                        else:
                            response = await logged_client.get(
                                'https://fhbstat.com/football',
                                json={'page': page_number}
                            )
                        if response.status_code == 200:
                            df = self.parse_content(response.content)
                            now = datetime.now()
                            if not df[df['dt'] <= now].empty:
                                dfs.append(df[df['dt'] > now])
                                break
                            else:
                                dfs.append(df)
                    future_data = pd.concat(dfs)
                    data_records = future_data.to_dict(orient='records')
                    result_df_list = []
                    for d_r in data_records:
                        response = await logged_client.get(
                            'https://fhbstat.com/football',
                            params={
                                '9': d_r['9'],
                                '10': d_r['10'],
                            }
                        )
                        df_9_10 = self.parse_content(response.content)
                        count_rows, _ = df_9_10.shape
                        d_r['Количество матчей'] = count_rows
                        if count_rows > 1:
                            result_df_list.append(d_r)
                    result = self.get_file_response(df_data=result_df_list)
                    return result
