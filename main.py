from datetime import datetime
from pathlib import Path

import pandas as pd
import tqdm
import yaml
from bs4 import BeautifulSoup
from loguru import logger
from nicegui import ui
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from beta_baza import parce_bet_baza
from config import URL

logger.add('app.log')


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


@ui.page('/parse')
async def parce():
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
        await page.goto('su')
        await page.wait_for_load_state()
        if page.url != URL + 'su':
            await page.goto('su')
            await page.wait_for_load_state()
        await page.wait_for_selector(
            '//table[@class="coupon-row-item"]',
            timeout=180000
        )
        await page.get_by_text('Футбол').first.click()
        await page.get_by_text('24 часа').first.click()
        need_scroll = True
        attempts = 5
        content = ''
        while need_scroll:
            await page.mouse.wheel(0, 1700)
            await page.wait_for_load_state()
            await page.wait_for_timeout(2000)
            if content == await page.content():
                if attempts:
                    attempts -= 1
                else:
                    need_scroll = False
            else:
                content = await page.content()
        soup = BeautifulSoup(await page.content(), 'html.parser')
        tables = soup.find_all(
            lambda tag: tag.name == 'table' and tag.get('class') == ['coupon-row-item']
        )
        df_data = []
        players_links = set()
        for table in tables:
            name_column = table.find('td', attrs={'class': 'first'})
            players = name_column.find_all('a', attrs={'class': 'member-link'})
            for i, player in enumerate(players, 1):
                players_links.add(player.attrs.get('href'))
        print(f'Количество ссылок: {len(players_links)}')
        for player_link in tqdm.tqdm(players_links):
            df_data_dict = dict()
            await page.goto(player_link)
            await page.wait_for_load_state()
            await page.wait_for_selector(
                '//div[@class="block-market-wrapper"]',
                timeout=180000
            )
            soup = BeautifulSoup(await page.content(), 'html.parser')

            tables = soup.find_all(lambda tag: tag.name == 'table' and tag.get('class') == ['coupon-row-item'])
            for table in tables:
                name_column = table.find('td', attrs={'class': 'first'})
                players = name_column.find_all('a', attrs={'class': 'member-link'})
                players_names = []
                for i, player in enumerate(players, 1):
                    players_names.append(f'{i}. {player.text.replace("\n", "")}')
            name = '\n'.join(players_names)
            if table.find('div', attrs={'class': 'date-wrapper'}):
                date_game = table.find('div', attrs={'class': 'date-wrapper'}).text
            else:
                date_game = None
            div_elements = soup.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['block-market-wrapper']
            )
            if len(div_elements) < 5:
                continue
            results, head_starts, totals, goals, times = div_elements

            # Результаты
            results_visible_div = results.find(
                lambda tag: tag.name == 'div' and tag.get('class') == ['market-inline-block-table-wrapper']
            )
            results_table = results_visible_div.find('table', class_='td-border')
            results_values = [tr.text for tr in results_table.find_all(
                lambda tag: tag.name == 'span' and sorted(tag.get('class')) == sorted(['selection-link', 'active-selection'])
            )]
            if len(results_values) < 6:
                continue

            P1, X, P2, _1X, _12, _2X = results_values

            # Форы
            head_starts_visible_div = head_starts.find(
                lambda tag: tag.name == 'div' and tag.get('class') == ['market-inline-block-table-wrapper']
            )
            head_starts_table = head_starts_visible_div.find('table', class_='td-border')
            head_starts_0 = head_starts_table.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['coeff-value'] and '(0)' in tag.text
            )
            if len(head_starts_0) < 2:
                continue

            head_starts_0_parents = [head_start_0.parent for head_start_0 in head_starts_0]
            head_starts_0_values = [
                head_start_0_parent.find('div', class_='coeff-price').span.text
                for head_start_0_parent in head_starts_0_parents
            ]
            F1_0, F2_0 = head_starts_0_values

            # Тотал голов
            totals_visible_div = totals.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['market-inline-block-table-wrapper']
            )
            totals_table = totals_visible_div[0].find('table', class_='td-border')
            totals_0 = totals_table.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['coeff-value'] and '(2.5)' in tag.text
            )
            if len(totals_0) < 2:
                continue

            totals_0_parents = [head_start_0.parent for head_start_0 in totals_0]
            totals_0_values = [
                head_start_0_parent.find('div', class_='coeff-price').span.text
                for head_start_0_parent in totals_0_parents
            ]
            TM_25, TB_25 = totals_0_values

            it_1_totals_table = totals_visible_div[1].find('table', class_='td-border')
            it_1_totals_0 = it_1_totals_table.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['coeff-value'] and '(1.5)' in tag.text
            )
            if len(it_1_totals_0) < 2:
                continue

            it_1_totals_0_parents = [it_1_head_start_0.parent for it_1_head_start_0 in it_1_totals_0]
            it_1_totals_0_values = [
                head_start_0_parent.find('div', class_='coeff-price').span.text
                for head_start_0_parent in it_1_totals_0_parents
            ]
            IT1_men_15, IT1_bol_15 = it_1_totals_0_values

            it_2_totals_table = totals_visible_div[2].find('table', class_='td-border')
            it_2_totals_0 = it_2_totals_table.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['coeff-value'] and '(1.5)' in tag.text
            )
            if len(it_2_totals_0) < 2:
                continue

            it_2_totals_0_parents = [it_2_head_start_0.parent for it_2_head_start_0 in it_2_totals_0]
            it_2_totals_0_values = [
                head_start_0_parent.find('div', class_='coeff-price').span.text
                for head_start_0_parent in it_2_totals_0_parents
            ]
            IT2_men_15, IT2_bol_15 = it_2_totals_0_values

            # Голы
            name_players = name.split('\n')
            name_players = [name_players[0].replace('1. ', ''), name_players[1].replace('2. ', '')]

            if not goals.find(lambda tag: tag.text == f'{name_players[0]} забьет'):
                continue
            K1_win = goals.find(lambda tag: tag.text == f'{name_players[0]} забьет').parent
            K1_win_yes_no = K1_win.parent.find_all(
                lambda tag: tag.name == 'td' and sorted(tag.get('class')) == sorted(['price', 'height-column-with-price'])
            )
            K1_win_yes, K1_win_no = [k1_win_yes_no.span.text for k1_win_yes_no in K1_win_yes_no]

            K2_win = goals.find(lambda tag: tag.text == f'{name_players[1]} забьет').parent
            K2_win_yes_no = K2_win.parent.find_all(
                lambda tag: tag.name == 'td' and sorted(tag.get('class')) == sorted(['price', 'height-column-with-price'])
            )
            K2_win_yes, K2_win_no = [k2_win_yes_no.span.text for k2_win_yes_no in K2_win_yes_no]

            ALL_win = goals.find(lambda tag: tag.text == 'Обе команды забьют').parent
            ALL_win_yes_no = ALL_win.parent.find_all(
                lambda tag: tag.name == 'td' and sorted(tag.get('class')) == sorted(['price', 'height-column-with-price'])
            )
            ALL_win_yes, ALL_win_no = [all_win_yes_no.span.text for all_win_yes_no in ALL_win_yes_no]

            ALL_tiems = goals.find(lambda tag: tag.text == 'Голы в обоих таймах').parent
            ALL_tiems_yes_no = ALL_tiems.parent.find_all(
                lambda tag: tag.name == 'td' and sorted(tag.get('class')) == sorted(['price', 'height-column-with-price'])
            )
            ALL_tiems_yes, ALL_tiems_no = [all_tiems_yes_no.span.text for all_tiems_yes_no in ALL_tiems_yes_no]

            K1_win_1_time = goals.find(lambda tag: tag.text == f'{name_players[0]} забьет, 1-й тайм').parent
            K1_win_1_time_yes_no = K1_win_1_time.parent.find_all(
                lambda tag: tag.name == 'td' and sorted(tag.get('class')) == sorted(['price', 'height-column-with-price'])
            )
            K1_win_1_time_yes, K1_win_1_time_no = [_K1_win_1_time_yes_no.span.text for _K1_win_1_time_yes_no in K1_win_1_time_yes_no]

            K1_win_2_time = goals.find(lambda tag: tag.text == f'{name_players[0]} забьет, 2-й тайм').parent
            K1_win_2_time_yes_no = K1_win_2_time.parent.find_all(
                lambda tag: tag.name == 'td' and sorted(tag.get('class')) == sorted(['price', 'height-column-with-price'])
            )
            K1_win_2_time_yes, K1_win_2_time_no = [_K1_win_2_time_yes_no.span.text for _K1_win_2_time_yes_no in K1_win_2_time_yes_no]

            K2_win_1_time = goals.find(lambda tag: tag.text == f'{name_players[1]} забьет, 1-й тайм').parent
            K2_win_1_time_yes_no = K2_win_1_time.parent.find_all(
                lambda tag: tag.name == 'td' and sorted(tag.get('class')) == sorted(['price', 'height-column-with-price'])
            )
            K2_win_1_time_yes, K2_win_1_time_no = [_K2_win_1_time_yes_no.span.text for _K2_win_1_time_yes_no in K2_win_1_time_yes_no]

            K2_win_2_time = goals.find(lambda tag: tag.text == f'{name_players[1]} забьет, 2-й тайм').parent
            K2_win_2_time_yes_no = K2_win_2_time.parent.find_all(
                lambda tag: tag.name == 'td' and sorted(tag.get('class')) == sorted(['price', 'height-column-with-price'])
            )
            K2_win_2_time_yes, K2_win_2_time_no = [_K2_win_2_time_yes_no.span.text for _K2_win_2_time_yes_no in K2_win_2_time_yes_no]

            # Таймы
            times_elements = times.find_all(lambda tag: tag.name == 'div' and tag.get('class') == ['market-inline-block-table-wrapper'])
            if len(times_elements) < 10:
                continue
            results_1_time, win_head_start_1_time, goals_1_time, goals_it1_1_time, goals_it2_1_time, results_2_time, win_head_start_2_time, goals_2_time, goals_it1_2_time, goals_it2_2_time = times_elements

            results_table_1_time = results_1_time.find('table', class_='td-border')
            results_values_1_time = [tr.text for tr in results_table_1_time.find_all(
                lambda tag: tag.name == 'span' and sorted(tag.get('class')) == sorted(['selection-link', 'active-selection'])
            )]
            if len(results_values_1_time) < 6:
                continue

            goal_1_time_P1, goal_1_time_X, goal_1_time_P2, goal_1_time_1X, goal_1_time_12, goal_1_time_2X = results_values_1_time

            # --
            head_starts_table_1_time = win_head_start_1_time.find('table', class_='td-border')
            head_starts_0_1_time = head_starts_table_1_time.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['coeff-value'] and '(0)' in tag.text
            )
            if len(head_starts_0_1_time) < 2:
                continue

            head_starts_0_1_time_parents = [head_start_0.parent for head_start_0 in head_starts_0_1_time]
            head_starts_0_1_time_values = [
                head_start_0_parent.find('div', class_='coeff-price').span.text
                for head_start_0_parent in head_starts_0_1_time_parents
            ]
            F1_0_1_time, F2_0_1_time = head_starts_0_1_time_values

            # --
            totals_table_1_time = goals_1_time.find('table', class_='td-border')
            totals_0_1_time = totals_table_1_time.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['coeff-value'] and '(2.5)' in tag.text
            )
            if len(totals_0_1_time) < 2:
                continue

            totals_0_1_time_parents = [head_start_0.parent for head_start_0 in totals_0_1_time]
            totals_0_1_time_values = [
                head_start_0_parent.find('div', class_='coeff-price').span.text
                for head_start_0_parent in totals_0_1_time_parents
            ]
            TM_25_1_time, TB_25_1_time = totals_0_1_time_values

            it_1_totals_table_1_time = goals_it1_1_time.find('table', class_='td-border')
            it_1_totals_0_1_time = it_1_totals_table_1_time.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['coeff-value'] and '(1.5)' in tag.text
            )
            if len(it_1_totals_0_1_time) < 2:
                continue

            it_1_totals_0_1_time_parents = [it_1_head_start_0.parent for it_1_head_start_0 in it_1_totals_0_1_time]
            it_1_totals_0_1_time_values = [
                head_start_0_parent.find('div', class_='coeff-price').span.text
                for head_start_0_parent in it_1_totals_0_1_time_parents
            ]
            IT1_men_15_1_time, IT1_bol_15_1_time = it_1_totals_0_1_time_values

            it_2_totals_table_1_time = goals_it2_1_time.find('table', class_='td-border')
            it_2_totals_0_1_time = it_2_totals_table_1_time.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['coeff-value'] and '(1.5)' in tag.text
            )
            if len(it_2_totals_0_1_time) < 2:
                continue

            it_2_totals_0_1_time_parents = [it_2_head_start_0.parent for it_2_head_start_0 in it_2_totals_0_1_time]
            it_2_totals_0_1_time_values = [
                head_start_0_parent.find('div', class_='coeff-price').span.text
                for head_start_0_parent in it_2_totals_0_1_time_parents
            ]
            IT2_men_15_1_time, IT2_bol_15_1_time = it_2_totals_0_1_time_values

            # 2 тайм
            results_table_2_time = results_2_time.find('table', class_='td-border')
            results_values_2_time = [tr.text for tr in results_table_2_time.find_all(
                lambda tag: tag.name == 'span' and sorted(tag.get('class')) == sorted(['selection-link', 'active-selection'])
            )]
            if len(results_values_2_time) < 6:
                continue

            goal_2_time_P1, goal_2_time_X, goal_2_time_P2, goal_2_time_1X, goal_2_time_12, goal_2_time_2X = results_values_2_time

            # --
            head_starts_table_2_time = win_head_start_2_time.find('table', class_='td-border')
            head_starts_0_2_time = head_starts_table_2_time.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['coeff-value'] and '(0)' in tag.text
            )
            if len(head_starts_0_2_time) < 2:
                continue

            head_starts_0_2_time_parents = [head_start_0.parent for head_start_0 in head_starts_0_2_time]
            head_starts_0_2_time_values = [
                head_start_0_parent.find('div', class_='coeff-price').span.text
                for head_start_0_parent in head_starts_0_2_time_parents
            ]
            F1_0_2_time, F2_0_2_time = head_starts_0_2_time_values

            # --
            totals_table_2_time = goals_2_time.find('table', class_='td-border')
            totals_0_2_time = totals_table_2_time.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['coeff-value'] and '(2.5)' in tag.text
            )
            if len(totals_0_2_time) < 2:
                continue

            totals_0_2_time_parents = [head_start_0.parent for head_start_0 in totals_0_2_time]
            totals_0_2_time_values = [
                head_start_0_parent.find('div', class_='coeff-price').span.text
                for head_start_0_parent in totals_0_2_time_parents
            ]
            TM_25_2_time, TB_25_2_time = totals_0_2_time_values

            it_1_totals_table_2_time = goals_it1_2_time.find('table', class_='td-border')
            it_1_totals_0_2_time = it_1_totals_table_2_time.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['coeff-value'] and '(1.5)' in tag.text
            )
            if len(it_1_totals_0_2_time) < 2:
                continue

            it_1_totals_0_2_time_parents = [it_1_head_start_0.parent for it_1_head_start_0 in it_1_totals_0_2_time]
            it_1_totals_0_2_time_values = [
                head_start_0_parent.find('div', class_='coeff-price').span.text
                for head_start_0_parent in it_1_totals_0_2_time_parents
            ]
            IT1_men_15_2_time, IT1_bol_15_2_time = it_1_totals_0_2_time_values

            it_2_totals_table_2_time = goals_it2_2_time.find('table', class_='td-border')
            it_2_totals_0_2_time = it_2_totals_table_2_time.find_all(
                lambda tag: tag.name == 'div' and tag.get('class') == ['coeff-value'] and '(1.5)' in tag.text
            )
            if len(it_2_totals_0_2_time) < 2:
                continue

            it_2_totals_0_2_time_parents = [it_2_head_start_0.parent for it_2_head_start_0 in it_2_totals_0_2_time]
            it_2_totals_0_2_time_values = [
                head_start_0_parent.find('div', class_='coeff-price').span.text
                for head_start_0_parent in it_2_totals_0_2_time_parents
            ]
            IT2_men_15_2_time, IT2_bol_15_2_time = it_2_totals_0_2_time_values

            df_data_dict['Ссылка'] = URL + player_link[1:]
            df_data_dict['Название'] = name
            df_data_dict['Дата'] = date_game
            df_data_dict['П1'] = P1
            df_data_dict['Ничья'] = X
            df_data_dict['П2'] = P2
            df_data_dict['П1 или ничья'] = _1X
            df_data_dict['П1 или П2'] = _12
            df_data_dict['П2 или ничья'] = _2X
            df_data_dict['Ф1 (0)'] = F1_0
            df_data_dict['Ф2 (0)'] = F2_0
            df_data_dict['ТМ (2.5)'] = TM_25
            df_data_dict['ТБ (2.5)'] = TB_25
            df_data_dict['ИТ1 меньше (1.5)'] = IT1_men_15
            df_data_dict['ИТ1 больше (1.5)'] = IT1_bol_15
            df_data_dict['ИТ2 меньше (1.5)'] = IT2_men_15
            df_data_dict['ИТ2 больше (1.5)'] = IT2_bol_15
            df_data_dict['К1 забьет Да'] = K1_win_yes
            df_data_dict['К1 забьет Нет'] = K1_win_no
            df_data_dict['К2 забьет Да'] = K2_win_yes
            df_data_dict['К2 забьет Нет'] = K2_win_no
            df_data_dict['Обе команды забьют Да'] = ALL_win_yes
            df_data_dict['Обе команды забьют Нет'] = ALL_win_no
            df_data_dict['Голы в обоих таймах Да'] = ALL_tiems_yes
            df_data_dict['Голы в обоих таймах Нет'] = ALL_tiems_no
            df_data_dict['К1 забьет, 1-й тайм Да'] = K1_win_1_time_yes
            df_data_dict['К1 забьет, 1-й тайм Нет'] = K1_win_1_time_no
            df_data_dict['К1 забьет, 2-й тайм Да'] = K1_win_2_time_yes
            df_data_dict['К1 забьет, 2-й тайм Нет'] = K1_win_2_time_no
            df_data_dict['К2 забьет, 1-й тайм Да'] = K2_win_1_time_yes
            df_data_dict['К2 забьет, 1-й тайм Нет'] = K2_win_1_time_no
            df_data_dict['К2 забьет, 2-й тайм Да'] = K2_win_2_time_yes
            df_data_dict['К2 забьет, 2-й тайм Нет'] = K2_win_2_time_no

            df_data_dict['П1, 1-й тайм'] = goal_1_time_P1
            df_data_dict['Ничья, 1-й тайм'] = goal_1_time_X
            df_data_dict['П2, 1-й тайм'] = goal_1_time_P2
            df_data_dict['П1 или ничья, 1-й тайм'] = goal_1_time_1X
            df_data_dict['П1 или П2, 1-й тайм'] = goal_1_time_12
            df_data_dict['П2 или ничья, 1-й тайм'] = goal_1_time_2X
            df_data_dict['Ф1 (0), 1-й тайм'] = F1_0_1_time
            df_data_dict['Ф2 (0), 1-й тайм'] = F2_0_1_time
            df_data_dict['ТМ (2.5), 1-й тайм'] = TM_25_1_time
            df_data_dict['ТБ (2.5), 1-й тайм'] = TB_25_1_time
            df_data_dict['ИТ1 меньше (1.5), 1-й тайм'] = IT1_men_15_1_time
            df_data_dict['ИТ1 больше (1.5), 1-й тайм'] = IT1_bol_15_1_time
            df_data_dict['ИТ2 меньше (1.5), 1-й тайм'] = IT2_men_15_1_time
            df_data_dict['ИТ2 больше (1.5), 1-й тайм'] = IT2_bol_15_1_time

            df_data_dict['П1, 2-й тайм'] = goal_2_time_P1
            df_data_dict['Ничья, 2-й тайм'] = goal_2_time_X
            df_data_dict['П2, 2-й тайм'] = goal_2_time_P2
            df_data_dict['П1 или ничья, 2-й тайм'] = goal_2_time_1X
            df_data_dict['П1 или П2, 2-й тайм'] = goal_2_time_12
            df_data_dict['П2 или ничья, 2-й тайм'] = goal_2_time_2X
            df_data_dict['Ф1 (0), 2-й тайм'] = F1_0_2_time
            df_data_dict['Ф2 (0), 2-й тайм'] = F2_0_2_time
            df_data_dict['ТМ (2.5), 2-й тайм'] = TM_25_2_time
            df_data_dict['ТБ (2.5), 2-й тайм'] = TB_25_2_time
            df_data_dict['ИТ1 меньше (1.5), 2-й тайм'] = IT1_men_15_2_time
            df_data_dict['ИТ1 больше (1.5), 2-й тайм'] = IT1_bol_15_2_time
            df_data_dict['ИТ2 меньше (1.5), 2-й тайм'] = IT2_men_15_2_time
            df_data_dict['ИТ2 больше (1.5), 2-й тайм'] = IT2_bol_15_2_time
            df_data.append(
                df_data_dict
            )
        if df_data:
            print(f'Собрано данных: {len(df_data)}')
            df = pd.DataFrame.from_records(df_data)
            df.to_excel(f'{datetime.now().isoformat()}.xlsx', index=False)
        else:
            print('Не собрали данных')
        await browser.close()


if __name__ in {"__main__", "__mp_main__"}:
    ui.page_title('Parser bet')
    ui.input('Ссылка:', value=get_saved_url(), on_change=lambda elem: save_url(elem.value))
    ui.label('Нажмите "Начать" для запуска парсера')
    ui.link('Получить excel', parce)
    ui.link('Получить данные Бет-База', '/parse_bet_baza', new_tab=True)
    ui.run(
        show=False
    )
