from threading import Event
from typing import Optional

from fastapi.responses import RedirectResponse
from nicegui import app, ui

from base import BrowserManager
from beta_baza import parse_bet_baza  # noqa:F401
from config import settings
from parsers.fhbstat import FHBParser
from parsers.marathonbet import MarathonbetParser
from parsers.xlite import XLiteParser
from utils import AuthMiddleware

app.add_middleware(AuthMiddleware)

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

    async def add_rounded_select(element):
        with ui.row():
            ui.select(
                list(filter(lambda x: x >= 11 and x not in fhbstat_parser.rounded_fields, labels)),
                on_change=add_rounded_field
            )

    async def set_field(number_field):
        async def change_rounded_field(element):
            if element.value:
                fhbstat_parser.rounded_fields[number_field] = element.value
            else:
                fhbstat_parser.rounded_fields.pop(element.sender.label)
        return change_rounded_field

    async def add_rounded_field(element):
        with ui.row():
            ui.number(label=element.sender.label, value=0.0001, on_change=await set_field(element.value))
            fhbstat_parser.rounded_fields[element.value] = 0.0001
            ui.button('Добавить', on_click=add_rounded_select)

    async def _get_filters(element):
        with filter_row:
            filter_row.clear()
            download_button.set_text(text=f'Скачать excel ({element.value or "..."})')
            if element.value:
                labels_name = {
                    1: 'День',
                    2: 'Месяц',
                    3: 'Год',
                }
                with ui.grid(columns=3):
                    for label in labels[:3]:
                        ui.label(labels_name.get(label, label))
                    fields = [ui.number(label=label, on_change=fhbstat_parser.add_filter) for label in labels[:3]]
                    for field in fields:
                        if field.label not in (1, 2, 3):
                            field.disable()
                with ui.row():
                    ui.label('Выберите поля')
                    await add_rounded_select(None)

    labels = list(range(1, 132))

    with ui.row():
        ui.label('Выберите вид спорта')
        ui.select(
            ['Футбол Исход между собой'],
            label='Выберите вид спорта',
            on_change=_get_filters,
            clearable=True
        )
    filter_row = ui.card()
    with ui.row():
        ui.input('Email').bind_value(fhbstat_parser, 'email')
        ui.input('Пароль', password=True, password_toggle_button=True).bind_value(fhbstat_parser, 'password')
    ui.label('Статус: Вычисляем').bind_text_from(fhbstat_parser, 'status')
    download_button = ui.button('Скачать excel (...)', on_click=download('/parse_fhbstat'))


@ui.page('/login')
def login(redirect_to: str = '/') -> Optional[RedirectResponse]:
    def try_login() -> None:  # local function to avoid passing username and password as arguments
        if password.value == settings.ADMIN_PASSWORD and username.value == settings.ADMIN_USERNAME:
            app.storage.user.update({'username': username.value, 'authenticated': True})
            ui.navigate.to(redirect_to)  # go back to where the user wanted to go
        else:
            ui.notify('Wrong username or password', color='negative')

    if app.storage.user.get('authenticated', False):
        return RedirectResponse('/')
    with ui.card().classes('absolute-center'):
        username = ui.input('Username').on('keydown.enter', try_login)
        password = ui.input('Password', password=True, password_toggle_button=True).on('keydown.enter', try_login)
        ui.button('Log in', on_click=try_login)
    return None


if __name__ in {"__main__", "__mp_main__"}:
    ui.page_title('Parser bet')
    ui.link('Получить excel', '/parse_page', new_tab=True)
    ui.link('Получить данные Бет-База', '/parse_bet_baza', new_tab=True)
    ui.link('Получить 1xlite', '/xlite_page', new_tab=True)
    ui.link('fhbstat', '/fhbstat_page', new_tab=True)
    ui.run(
        show=False,
        # port=8081,
        storage_secret=settings.STORAGE_SECRET
    )
