from abc import ABC, abstractmethod
from datetime import timedelta
from pathlib import Path
from threading import Event
from time import time

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from utils import get_saved_url, save_url


class ParserBase(ABC):

    @abstractmethod
    def start(self):
        raise NotImplementedError()

    @abstractmethod
    def stop(self):
        raise NotImplementedError()

    @abstractmethod
    def parse(self):
        raise NotImplementedError()


class Parser(ParserBase):
    def __init__(self, is_running: Event):
        self._value = None
        self.radio_period = '24 часа'
        self._count_links = None
        self._count_processed_links = None
        self._elapsed_time = None
        self._eta = None
        self._status = None
        self._is_running = is_running

        self._url_config_file = f'{self.__class__.__name__.lower()}_url.yaml'
        self._url = None

    @property
    def url(self):
        _url = None
        if self._url:
            _url = self._url
        else:
            _url = get_saved_url(self._url_config_file)
        if _url and not _url.endswith('/'):
            if _url:
                _url += '/'
        self._url = _url
        return self._url

    @url.setter
    def url(self, new_url):
        if new_url:
            self._url = new_url
            save_url(self._url_config_file, new_url)

    @property
    def is_running(self):
        return self._is_running.is_set()

    def start(self):
        self._elapsed_time = time()

    def stop(self):
        self._count_processed_links = None
        self._count_links = None
        self._elapsed_time = None
        self._eta = None
        self._status = None

    @property
    def status(self):
        if self._status:
            return f'Статус: {self._status}'
        else:
            return 'Статус: --'

    @status.setter
    def status(self, value: str):
        if value:
            self._status = value
        else:
            self._status = None

    @property
    def elapsed_time(self):
        if self._elapsed_time:
            result = []
            td = timedelta(seconds=time() - self._elapsed_time)
            hours, remainder = divmod(td.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            if hours:
                result.append(f'{hours} ч.')
            if minutes:
                result.append(f'{minutes} мин.')
            if seconds:
                result.append(f'{seconds} сек.')
            return f'Прошло {", ".join(result)}'
        else:
            return 'Прошло -- сек.'

    @property
    def count_links(self):
        if self._count_links:
            return f'Количество ссылок: {str(self._count_links)}'
        else:
            if self.is_running:
                return 'Количество ссылок: Вычисляем'
            else:
                return 'Количество ссылок: -'

    @count_links.setter
    def count_links(self, value: int):
        if value:
            self._count_links = value
        else:
            self._count_links = None

    @property
    def count_processed_links(self):
        if self._count_processed_links and self._count_links:
            percent = round(self._count_processed_links / self._count_links * 100, 2)
            return f'Обработано ссылок: {str(self._count_processed_links)} ({percent} %)'
        else:
            if self.is_running:
                return 'Обработано ссылок: Вычисляем'
            else:
                return 'Обработано ссылок: -'

    @count_processed_links.setter
    def count_processed_links(self, value: int):
        if value:
            self._count_processed_links = value
        else:
            self._count_processed_links = None

    def tqdm(self, links):
        start = time()
        for i, link in enumerate(links):
            yield link
            self.count_processed_links = i
            end = time()
            delta = end - start
            self._eta = (len(links) - (i + 1)) * delta
            start = end

    @property
    def eta(self):
        if self._eta:
            result = []
            td = timedelta(seconds=self._eta)
            hours, remainder = divmod(td.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            if hours:
                result.append(f'{hours} ч.')
            if minutes:
                result.append(f'{minutes} мин.')
            if seconds:
                result.append(f'{seconds} сек.')
            return f'Осталось примерно {", ".join(result)}'
        else:
            return 'Осталось -- сек.'


class BrowserManager:
    def __init__(self, is_running: Event, parser: Parser):
        self._is_running = is_running
        self._parser = parser
        self._ctx_browser = None

    @property
    def parser(self):
        return self._parser

    @property
    def is_running(self):
        return self._is_running.is_set()

    async def __aenter__(self):
        if not self.is_running:
            singleton_lock_file = Path('browser/SingletonLock')
            if singleton_lock_file.exists():
                singleton_lock_file.unlink()
            self._ctx_browser = Stealth().use_async(async_playwright())
            p = await self._ctx_browser.__aenter__()
            browser = await p.chromium.launch_persistent_context(
                user_data_dir='browser/',
                channel='chrome',
                headless=False,
                args=[
                    '--start-maximized',
                    '--disable-blink-features=AutomationControlled'
                ],
                base_url=self.parser.url,
                screen={
                    "width": 1920,
                    "height": 1080
                },
                viewport={
                    "width": 1920,
                    "height": 1080
                }
            )
            self._is_running.set()
            return browser

    async def parse(self, browser):
        self.parser.start()
        result = await self.parser.parse(browser)
        self.parser.stop()
        return result

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._is_running.clear()
        await self._ctx_browser.__aexit__(exc_type, exc_val, exc_tb)
