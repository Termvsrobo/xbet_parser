from threading import Event

from nicegui import app, ui

from base import BrowserManager
from beta_baza import parse_bet_baza  # noqa:F401
from parsers.fhbstat import FHBParser
from parsers.marathonbet import MarathonbetParser
from parsers.xlite import XLiteParser

is_running = Event()


marathonbet_parser = MarathonbetParser(is_running=is_running)
xlite_parser = XLiteParser(is_running=is_running)
fhbstat_parser = FHBParser(is_running=is_running)


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
    @app.get('/parse_fhbstat')
    async def _parse_fhbstat():
        b_manager = BrowserManager(is_running=is_running, parser=fhbstat_parser)
        async with b_manager as browser:
            if browser:
                response = await b_manager.parse(browser)
                return response

    async def _get_filters(element):
        with filter_row:
            filter_row.clear()
            download_button.set_text(text=f'Скачать excel ({element.value or "..."})')
            if element.value:
                labels = [
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    18,
                    19,
                    20,
                    21,
                    22,
                    23,
                    25,
                    26,
                    27,
                    32,
                    33,
                    34,
                    28,
                    29,
                    30,
                    35,
                    36,
                    37,
                    38,
                    39,
                    127,
                    113,
                    114,
                    128,
                    92,
                    95,
                    129,
                    44,
                    45,
                    46,
                    130,
                    47,
                    48,
                    49,
                    131,
                    50,
                    51,
                    52,
                    53,
                    54,
                    55,
                    56,
                    57,
                    58,
                ]
                labels_name = {
                    1: 'День',
                    2: 'Месяц',
                    3: 'Год',
                }
                with ui.grid(columns=60):
                    for label in labels:
                        ui.label(labels_name.get(label, label)).classes('rotate-270')
                    fields = [ui.input(label=label, on_change=fhbstat_parser.add_filter) for label in labels]
                    for field in fields:
                        if field.label not in (1, 2, 3):
                            field.disable()
    with ui.row():
        ui.label('Выберите вид спорта')
        ui.select(
            ['Футбол Исход между собой'],
            label='Выберите вид спорта',
            on_change=_get_filters,
            clearable=True
        )
    filter_row = ui.row()
    with ui.row():
        ui.input('Email').bind_value(fhbstat_parser, 'email')
        ui.input('Пароль', password=True, password_toggle_button=True).bind_value(fhbstat_parser, 'password')
    ui.label('Статус: Вычисляем').bind_text_from(fhbstat_parser, 'status')
    download_button = ui.button('Скачать excel (...)', on_click=download('/parse_fhbstat'))


if __name__ in {"__main__", "__mp_main__"}:
    ui.page_title('Parser bet')
    ui.link('Получить excel', '/parse_page', new_tab=True)
    ui.link('Получить данные Бет-База', '/parse_bet_baza', new_tab=True)
    ui.link('Получить 1xlite', '/xlite_page', new_tab=True)
    ui.link('fhbstat', '/fhbstat_page', new_tab=True)
    ui.run(
        show=False
    )
