import datetime
from pathlib import Path
from urllib.parse import urlunparse

import pytest
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from parsers.marathonbet import parse as marathonbet_parse


@pytest.mark.asyncio
async def test_parser():
    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch_persistent_context(
            user_data_dir='browser/',
            channel='chrome',
            headless=False,
            args=[
                '--start-maximized',
                '--disable-blink-features=AutomationControlled'
            ],
        )
        page = await browser.new_page()

        test_page_file = (Path(__file__).parent / Path('data') / Path('test_1.html')).as_posix()
        test_page_url = urlunparse(('file', '', test_page_file, '', '', ''))
        await page.goto(test_page_url)
        df_data_dict = marathonbet_parse(await page.content(), test_page_url)
        await page.close()

        assert df_data_dict == {
            "Ссылка": "file:///home/termvsrobo/work/parser_bet/tests/data/test_1.html",
            "Название": "1. Латвия\n2. Сербия",
            "Дата": datetime.datetime(2025, 9, 7, 16, 0),
            "1": "10.75",
            "Х": "5.10",
            "2": "1.33",
            "1Х": "3.48",
            "12": "1.186",
            "Х2": "1.057",
            "Ф1(0)": "7.10",
            "Ф2(0)": "1.058",
            "ТМ(2.5)": "2.03",
            "ТБ(2.5)": "1.80",
            "ИТМ1(1.5)": "1.092",
            "ИТБ1(1.5)": "6.95",
            "ИТМ2(1.5)": "2.58",
            "ИТБ2(1.5)": "1.49",
            "ОЗ Да": "2.21",
            "ОЗ Нет": "1.59",
            "Гол оба тайма Да": "1.67",
            "Гол оба тайма Нет": "2.07",
            "_1_1": "8.90",
            "_1_Х": "2.42",
            "_1_2": "1.78",
            "_1_1Х": "1.909",
            "_1_12": "1.49",
            "_1_Х2": "1.027",
            "_1_Ф1(0)": "5.50",
            "_1_Ф2(0)": "1.098",
            "_1_ТМ(1.5)": "1.46",
            "_1_ТБ(1.5)": "2.59",
            "_1_ИТМ1(0.5)": "1.23",
            "_1_ИТБ1(0.5)": "3.72",
            "_1_ИТМ2(0.5)": None,
            "_1_ИТБ2(0.5)": None,
            "_2_1": "8.50",
            "_2_Х": "2.97",
            "_2_2": "1.58",
            "_2_1Х": "2.21",
            "_2_12": "1.34",
            "_2_Х2": "1.032",
            "_2_Ф1(0)": "5.70",
            "_2_Ф2(0)": "1.089",
            "_2_ТМ(1.5)": "1.77",
            "_2_ТБ(1.5)": "2.02",
            "_2_ИТМ1(0.5)": "1.32",
            "_2_ИТБ1(0.5)": "3.10",
            "_2_ИТМ2(0.5)": None,
            "_2_ИТБ2(0.5)": None,
        }
