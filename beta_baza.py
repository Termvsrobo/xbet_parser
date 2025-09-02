from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytz
import yaml
from bs4 import BeautifulSoup
from fastapi.responses import FileResponse
from loguru import logger
from nicegui import app, ui
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

URL = 'https://bet-baza.pro/main'


logger.add('app.log')

_login = 'omsk-forex'
_password = '123456'


def get_saved_url():
    url = None
    saved_url = Path('saved_url.yaml')
    if saved_url.exists():
        with saved_url.open(encoding='utf-8') as f:
            data = yaml.safe_load(f)
            if data:
                url = data.get('URL')
    return url


def save_url(url):
    if url:
        saved_url = Path('saved_url.yaml')
        data = {'URL': url}
        with saved_url.open('w', encoding='utf-8') as f:
            yaml.dump(data, f)


@app.get('/parse_bet_baza')
async def parce_bet_baza():
    result = None
    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch_persistent_context(
            user_data_dir='browser/',
            channel='chrome',
            headless=False,
            args=[
                '--start-maximized',
                '--disable-blink-features=AutomationControlled'
            ],
            base_url=URL,
        )
        page = await browser.new_page()
        page.set_default_timeout(180000)
        await page.goto('/')
        await page.wait_for_load_state()
        clear_filter_btn = page.get_by_text('Очистить кф')
        next_page = page.get_by_text('Следующая')
        if await clear_filter_btn.count():
            await clear_filter_btn.click()
        else:
            login = page.locator('//input[@name="login"]')
            password = page.locator('//input[@name="password"]')
            enter_button = page.get_by_role('button', name='Войти', exact=True).first

            await login.fill(_login)
            await password.fill(_password)
            await enter_button.click()
            await page.wait_for_load_state()
            clear_filter_btn = page.get_by_text('Очистить кф')
            next_page = page.get_by_text('Следующая')
            await clear_filter_btn.click()
        await page.wait_for_selector('//div[@class="dataTables_info" and text()!="Записи с 0 до 0 из 0 записей"]')
        df_list = []
        process_next_page = True
        page_number = 1
        while process_next_page:
            head_table = page.locator('//table[@role="grid"]').first
            htdata = await head_table.locator('//tbody/tr').all()
            tdata_values = [
                [await _htdata.inner_text() for _htdata in await _htdata.locator('td').all()]
                for _htdata in htdata
            ]
            head = await head_table.locator('//thead/tr/th').all()
            names = [await _head.inner_text() for _head in head]
            process_next_page = not bool(list(filter(lambda x: x[names.index('Счёт')] != '', tdata_values)))
            targets = await page.locator('//span[@data-full>=1]').all()
            for target in targets:
                parent_target = target.locator('..').first
                prev_siblings = await parent_target.locator('//preceding-sibling::td').all()
                count = await prev_siblings[names.index('Счёт')].inner_text()
                if count != '':
                    continue
                _text = await parent_target.inner_text()
                if _text.startswith(' T '):
                    _text = _text.replace(' T', '')
                _text = _text.split('\n')
                for i in range(3):
                    _targets = await page.locator(f'//td[text()="{_text[0]}" and text()="{_text[1]}"]').all()
                    _el = _targets[0]
                    _parent_target = _el.first
                    siblings = await _parent_target.locator('//following-sibling::td').all()
                    await siblings[i].click()
                soup = BeautifulSoup(await page.content(), 'lxml')
                table = soup.find('table', attrs={'role': 'grid'})
                tdata = table.tbody.find_all('tr')
                tdata_rows = [[_td.attrs.get('class') == ['g'] for _td in _tdata.find_all('td')] for _tdata in tdata]
                tdata_values = [
                    [
                        _td.get_text('\n', True).replace(_td.span.text, '')
                        if _td.span else _td.get_text('\n', True)
                        for _td in _tdata.find_all('td')
                    ]
                    for _tdata in tdata
                ]
                tdata_values = [[_v.strip().replace('toto\nT\n', '') for _v in _r] for _r in tdata_values]
                _rows_data = list(zip(tdata_values, tdata_rows))
                if len(_rows_data) > 1:
                    _values = [_v[0] for _v in _rows_data]
                    _is_green = [_v[1] for _v in _rows_data]
                    _is_green_matrix = np.array(_is_green)
                    avg = _is_green_matrix[1:, 6:9].mean(axis=0).reshape(1, -1) * 100
                    df_values = pd.DataFrame(
                        columns=names[:6] + ['Количество двойников'],
                        data=[_values[0][:6] + [len(_rows_data) - 1]]
                    )
                    df_avg = pd.DataFrame(columns=names[6:9], data=avg.round(2))
                    df = pd.concat((df_values, df_avg), axis=1)
                    df_list.append(df)
                await clear_filter_btn.click()
                for i in range(page_number - 1):
                    await next_page.click()
                    await page.wait_for_load_state()
            if process_next_page:
                page_number += 1
                await next_page.click()
                await page.wait_for_load_state()
        await browser.close()
    if df_list:
        now_msk = datetime.now(tz=pytz.timezone('Europe/Moscow'))
        df = pd.concat(df_list, ignore_index=True)
        df.loc[:, 'Дата'] = pd.to_datetime(df.loc[:, 'Дата'], format='%d.%m.%y %H:%M').dt.tz_localize('Europe/Moscow')
        df['Дата'] = df['Дата'].astype('datetime64[ns, Europe/Moscow]')
        df = df[df['Дата'] > now_msk]
        df = df.sort_values(['Дата'])
        with pd.ExcelWriter(f'bet_baza_{now_msk.isoformat()}.xlsx', datetime_format='%d.%m.%y %H:%M') as writer:
            df['Дата'] = df['Дата'].dt.tz_localize(None)
            df.to_excel(writer, index=False)
        result = FileResponse(
            f'bet_baza_{now_msk.isoformat()}.xlsx',
            filename=f'bet_baza_{now_msk.isoformat()}.xlsx',
        )
    else:
        print('Нет данных')
    return result


if __name__ in {"__main__", "__mp_main__"}:
    ui.page_title('Аналитика')
    ui.label('Нажмите "Начать" для запуска парсера')
    ui.button('Начать', on_click=parce_bet_baza)
    ui.run(
        show=False
    )
