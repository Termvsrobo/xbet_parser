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
        if element:
            parent_row_id = element.sender.parent_slot.parent.id
            ui.select(
                list(filter(lambda x: x not in fhbstat_parser.rounded_fields[parent_row_id], labels.keys())),
                on_change=add_rounded_field
            )
        else:
            ui.select(
                list(labels.keys()),
                on_change=add_rounded_field
            )

    async def set_field(number_field):
        async def change_rounded_field(element):
            parent_row_id = element.sender.parent_slot.parent.id
            if element.value:
                fhbstat_parser.rounded_fields[parent_row_id][number_field] = element.value
            else:
                del fhbstat_parser.rounded_fields[parent_row_id][number_field]
                if not fhbstat_parser.rounded_fields[parent_row_id]:
                    del fhbstat_parser.rounded_fields[parent_row_id]
        return change_rounded_field

    async def add_rounded_field(element):
        field_type = labels.get(element.value)
        parent_row_id = element.sender.parent_slot.parent.id
        if issubclass(field_type, bool):
            ui.checkbox(value=True, on_change=await set_field(element.value)).props('disabled')
            fhbstat_parser.rounded_fields[parent_row_id][element.value] = True
        elif issubclass(field_type, float):
            ui.number(label=element.value, value=0.0001, on_change=await set_field(element.value))
            fhbstat_parser.rounded_fields[parent_row_id][element.value] = 0.0001
        else:
            ui.input(label=element.value, on_change=await set_field(element.value))
        ui.button('Добавить', on_click=add_rounded_select)

    async def _get_filters(element):
        with filter_row:
            filter_row.clear()
            download_button.set_text(text=f'Скачать excel ({element.value or "..."})')
            if element.value:
                with ui.row():
                    ui.label('Выберите поля')
                    await add_rounded_select(None)
                ui.button('Добавить фильтр', on_click=add_filter_card)

    async def add_filter_card(element):
        with filter_row:
            with ui.row():
                ui.label('Выберите поля')
                await add_rounded_select(None)
            ui.button('Добавить фильтр', on_click=add_filter_card)

    async def add_target_url(element):
        if element.value:
            fhbstat_parser.target_urls[element.sender] = element.value

    async def clear_filters(element):
        await _get_filters(fake_element)
        fhbstat_parser.rounded_fields.clear()
        fhbstat_parser.target_urls.clear()

    class FakeElement:
        def __init__(self, value):
            self.value = value
            self.sender = self

    labels = {i: fhbstat_parser.get_field_type(i) for i in range(1, fhbstat_parser.count_columns)}

    with ui.row():
        ui.input('Email').bind_value(fhbstat_parser, 'email')
        ui.input('Пароль', password=True, password_toggle_button=True).bind_value(fhbstat_parser, 'password')
    with ui.row().props('disabled data-value=15'):
        ui.label('Выберите вид спорта')
        ui.select(
            ['Футбол Исход'],
            label='Выберите вид спорта',
            on_change=_get_filters,
            clearable=True
        ).props('disabled')
    filter_row = ui.card()
    fake_element = FakeElement('test')
    ui.button('Очистить фильтр', on_click=clear_filters)
    ui.label('Ссылки вставлять только копированием/вставкой. НЕ ВВОДИТЬ ВРУЧНУЮ')
    for _ in range(1):
        with ui.row():
            if fhbstat_parser.target_urls:
                copy_target_urls = fhbstat_parser.target_urls.copy()
                fhbstat_parser.target_urls.clear()
                for value in copy_target_urls.values():
                    ui.input('Ссылка:', on_change=add_target_url).set_value(value)
            else:
                ui.input('Ссылка:', on_change=add_target_url)
    ui.label('Обработано ссылок: Вычисляем').bind_text(fhbstat_parser, 'count_processed_links')
    ui.label('Прошло секунд: Вычисляем').bind_text_from(fhbstat_parser, 'elapsed_time')
    ui.label('Осталось секунд: Вычисляем').bind_text_from(fhbstat_parser, 'eta')
    ui.label('Статус: Вычисляем').bind_text_from(fhbstat_parser, 'status')
    download_button = ui.button('Скачать excel (...)', on_click=download('/parse_fhbstat'))
    await _get_filters(fake_element)


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
        port=settings.PORT,
        storage_secret=settings.STORAGE_SECRET
    )
