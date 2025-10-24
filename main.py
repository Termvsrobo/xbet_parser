from itertools import count
from threading import Event

from nicegui import app, ui

from base import BrowserManager
from beta_baza import parse_bet_baza  # noqa:F401
from parsers.marathonbet import MarathonbetParser
from parsers.xlite import XLiteParser

is_running = Event()


marathonbet_parser = MarathonbetParser(is_running=is_running)
xlite_parser = XLiteParser(is_running=is_running)


@app.get('/parse')
async def parse():
    b_manager = BrowserManager(is_running=is_running, parser=marathonbet_parser)
    async with b_manager as browser:
        if browser:
            response = await b_manager.parse(browser)
            return response


@app.get('/parse_xlite')
async def parse_xlite():
    b_manager = BrowserManager(is_running=is_running, parser=xlite_parser)
    async with b_manager as browser:
        if browser:
            response = await b_manager.parse(browser)
            return response


def download(url):
    def wrapper():
        if not is_running.is_set():
            ui.download.from_url(url)
        else:
            ui.notify('В данный момент уже запущен процесс парсинга. Дождитесь его окончания, чтобы запустить новый')
    return wrapper


@ui.page('/parse_page')
async def parse_page():
    ui.page_title('Парсер марафонбет')
    ui.input('Ссылка:').bind_value(marathonbet_parser, 'url')
    ui.label('Выберите период')
    ui.radio(
        ['Всё время', '24 часа', 'Сегодня', '12 часов', '6 часов', '2 часа', '1 час'],
        value='24 часа'
    ).props('inline').bind_value(marathonbet_parser, 'radio_period')
    ui.label('Количество ссылок: Вычисляем').bind_text(marathonbet_parser, 'count_links')
    ui.label('Обработано ссылок: Вычисляем').bind_text(marathonbet_parser, 'count_processed_links')
    ui.label('Прошло секунд: Вычисляем').bind_text_from(marathonbet_parser, 'elapsed_time')
    ui.label('Осталось секунд: Вычисляем').bind_text_from(marathonbet_parser, 'eta')
    ui.label('Статус: Вычисляем').bind_text_from(marathonbet_parser, 'status')
    ui.button('Скачать excel', on_click=download('/parse'))


@ui.page('/xlite_page')
async def xlite_page():
    ui.page_title('Парсер 1xlite')
    ui.input('Ссылка:').bind_value(xlite_parser, 'url')
    ui.label('Выберите период')
    ui.radio(
        [
            'За всё время',
            'Ближайшие 24 часа',
            'Ближайшие 12 часов',
            'Ближайшие 6 часов',
            'Ближайшие 2 часа',
            'Ближайший час'
        ],
        value='Ближайшие 24 часа'
    ).props('inline').bind_value(xlite_parser, 'radio_period')
    ui.label('Количество ссылок: Вычисляем').bind_text(xlite_parser, 'count_links')
    ui.label('Обработано ссылок: Вычисляем').bind_text(xlite_parser, 'count_processed_links')
    ui.label('Прошло секунд: Вычисляем').bind_text_from(xlite_parser, 'elapsed_time')
    ui.label('Осталось секунд: Вычисляем').bind_text_from(xlite_parser, 'eta')
    ui.label('Статус: Вычисляем').bind_text_from(xlite_parser, 'status')
    ui.button('Скачать excel', on_click=download('/parse_xlite'))


@ui.page('/fhbstat_page')
async def fhbstat_page():
    def add_tab():
        i = next(number_click)
        with tabs:
            tab = ui.tab(f'Вкладка {i}')
        with panels:
            with ui.tab_panel(tab):
                ui.label(f'Панель с данными {i}')
            panels.set_value(tab)
        ui.notify('Добавлена новая вкладка с результатами поиска')
    number_click = count(0)
    with ui.row(align_items='center'):
        ui.label('Фильтр')
    with ui.card():
        with ui.grid(columns=12):
            for i in range(1, 37):
                with ui.row():
                    ui.input(f'Фильтр {i}')
    with ui.row(align_items='center'):
        ui.button('ПОИСК', on_click=add_tab)
    with ui.row(align_items='center'):
        ui.label('Вкладки')
    tabs = ui.tabs().classes('w-full').classes('w-full')
    panels = ui.tab_panels(tabs).classes('w-full')


if __name__ in {"__main__", "__mp_main__"}:
    ui.page_title('Parser bet')
    ui.link('Получить excel', '/parse_page', new_tab=True)
    ui.link('Получить данные Бет-База', '/parse_bet_baza', new_tab=True)
    ui.link('Получить 1xlite', '/xlite_page', new_tab=True)
    ui.link('fhbstat', '/fhbstat_page', new_tab=True)
    ui.run(
        show=False
    )
