from threading import Event
from typing import Optional

from fastapi.responses import RedirectResponse
from nicegui import app, ui

from base import BrowserManager
from beta_baza import parse_bet_baza  # noqa:F401
from config import settings
from parsers.fhbstat import FHBParser, FieldType
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

    @app.get('/download_filters')
    async def download_filters():
        response = fhbstat_parser.download_filters()
        return response

    def handle_upload(e):
        fhbstat_parser.upload_filters(e)
        filters.refresh()
        e.sender.reset()
        e.sender.delete()

    def upload():
        def wrapper():
            if not is_running.is_set():
                ui.upload(
                    on_upload=handle_upload
                )
            else:
                ui.notify('Запущен процесс парсинга. Дождитесь окончания и загрузите новыве фильтры!')
        return wrapper

    def add_rounded_select(element):
        if element:
            filter_id = int(element.sender.parent_slot.parent.props['filter_id'])
            ui.select(
                list(
                    filter(
                        lambda x: x not in fhbstat_parser.get_used_columns_by_filter(filter_id),
                        fhbstat_parser.columns
                    )
                ),
                on_change=add_rounded_field
            )
        else:
            ui.select(
                fhbstat_parser.columns,
                on_change=add_rounded_field
            )

    def set_field(number_field, is_priority=False):
        def change_rounded_field(element):
            filter_id = int(element.sender.parent_slot.parent.props['filter_id'])
            if element.value:
                if not is_priority:
                    fhbstat_parser.add_user_filter(filter_id=filter_id, column=number_field, filter_value=element.value)
                else:
                    fhbstat_parser.add_user_filter(filter_id=filter_id, column=number_field, priority=element.value)
            else:
                if not is_priority:
                    fhbstat_parser.remove_user_filter(filter_id=filter_id, column=number_field)
        return change_rounded_field

    def add_rounded_field(element):
        filter_id = int(element.sender.parent_slot.parent.props['filter_id'])
        old_column = element.sender.props.get('old_column')
        if old_column:
            _old_column = int(old_column)
            fhbstat_parser.remove_user_filter(filter_id=filter_id, column=_old_column)
        fhbstat_parser.add_user_filter(filter_id=filter_id, column=element.value)
        filters.refresh()

    def add_filter_card(element):
        with ui.row():
            ui.label('Выберите поля')
            with ui.row().props(f'filter_id={fhbstat_parser.get_filter_id()}'):
                add_rounded_select(None)

    def add_target_url(element):
        if element.value:
            fhbstat_parser.target_urls[element.sender.props['link_id']] = element.value
            link.refresh()

    def clear_filters(element):
        fhbstat_parser.user_filters.root.clear()
        fhbstat_parser.target_urls.clear()
        filters.refresh()

    @ui.refreshable
    def link():
        if fhbstat_parser.target_urls:
            for key, value in fhbstat_parser.target_urls.items():
                with ui.row():
                    ui.input('Ссылка:', value=value, on_change=add_target_url).props(f'link_id={int(key)}')
        else:
            for i in range(1):
                with ui.row():
                    ui.input('Ссылка:', on_change=add_target_url).props(f'link_id={i}')

    @ui.refreshable
    def filters():
        if fhbstat_parser.user_filters.root:
            with ui.row():
                for user_filter in fhbstat_parser.user_filters.root:
                    ui.label('Выберите поля')
                    with ui.row().props(f'filter_id={user_filter.filter_id}'):
                        for _filter in user_filter.filters:
                            _labels = list(
                                filter(
                                    lambda x: x not in fhbstat_parser.get_used_columns_by_filter(user_filter.filter_id),
                                    fhbstat_parser.columns
                                )
                            )
                            if int(_filter.column) not in _labels:
                                _labels.append(int(_filter.column))
                            select_field = ui.select(
                                sorted(
                                    _labels
                                ),
                                value=_filter.column,
                                on_change=add_rounded_field
                            )
                            select_field.props(f'old_column={_filter.column}')
                            field_type = fhbstat_parser.get_field_type(_filter.column)
                            if field_type is FieldType.BOOL:
                                ui.checkbox(value=True, on_change=set_field(_filter.column)).props('disabled')
                            elif field_type is FieldType.FLOAT:
                                ui.input(
                                    label=_filter.column,
                                    value=_filter.filter_value,
                                    on_change=set_field(_filter.column)
                                )
                                ui.select(
                                    [None] + list(range(1, 11)),
                                    label='Приоритет',
                                    value=_filter.priority,
                                    on_change=set_field(_filter.column, is_priority=True)
                                )
                            elif field_type is FieldType.TIME:
                                ui.input(
                                    label=_filter.column,
                                    value=_filter.filter_value,
                                    on_change=set_field(_filter.column)
                                )
                            else:
                                ui.input(label=_filter.column, on_change=set_field(_filter.column))
                        ui.button('Добавить', on_click=add_rounded_select)
                    ui.separator()
        else:
            with ui.row():
                next_filter_id = fhbstat_parser.get_filter_id()
                ui.label('Выберите поля')
                with ui.row().props(f'filter_id={next_filter_id}'):
                    select_field = ui.select(
                        fhbstat_parser.columns,
                        on_change=add_rounded_field
                    )
                    select_field.props('old_column=""')
        add_button = ui.button('Добавить фильтр', on_click=add_filter_card)
        if not bool(fhbstat_parser.user_filters.root):
            add_button.disable()
        else:
            add_button.enable()

    ui.page_title('FHB Stat')

    with ui.row():
        ui.input('Email').bind_value(fhbstat_parser, 'email')
        ui.input('Пароль', password=True, password_toggle_button=True).bind_value(fhbstat_parser, 'password')
    with ui.row():
        ui.input('Название файла (без расширения)').bind_value(fhbstat_parser, 'file_name')
    filters()
    ui.button('Очистить фильтр', on_click=clear_filters)
    with ui.input('Время с').bind_value(fhbstat_parser, 'from_time') as from_time:
        with ui.menu().props('no-parent-event') as menu:
            with ui.time().props('format24h').bind_value(from_time):
                with ui.row().classes('justify-end'):
                    ui.button('Close', on_click=menu.close).props('flat')
        with from_time.add_slot('append'):
            ui.icon('access_time').on('click', menu.open).classes('cursor-pointer')
    with ui.input('Время до').bind_value(fhbstat_parser, 'to_time') as to_time:
        with ui.menu().props('no-parent-event') as menu:
            with ui.time().props('format24h').bind_value(to_time):
                with ui.row().classes('justify-end'):
                    ui.button('Close', on_click=menu.close).props('flat')
        with to_time.add_slot('append'):
            ui.icon('access_time').on('click', menu.open).classes('cursor-pointer')
    link()
    ui.label('Обработано ссылок: Вычисляем').bind_text(fhbstat_parser, 'count_processed_links')
    ui.label('Прошло секунд: Вычисляем').bind_text_from(fhbstat_parser, 'elapsed_time')
    ui.label('Осталось секунд: Вычисляем').bind_text_from(fhbstat_parser, 'eta')
    ui.label('Статус: Вычисляем').bind_text_from(fhbstat_parser, 'status')
    ui.button('Скачать excel', on_click=download('/parse_fhbstat'))
    ui.button('Скачать json-фильтров', on_click=download('/download_filters'))
    ui.button('Загрузить фильтры из файла', on_click=upload())


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
