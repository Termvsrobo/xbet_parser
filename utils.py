import calendar
import locale
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence, Union

import pymongo.errors
import yaml
from dateutil.parser import parse, parserinfo
from fastapi import Request
from fastapi.responses import RedirectResponse
from nicegui import app
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.uri_parser import parse_uri
from starlette.middleware.base import BaseHTTPMiddleware


def parse_date_str(date: str):
    old_locale = locale.getlocale()
    locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')

    class LocaleParserInfo(parserinfo):
        WEEKDAYS = zip(calendar.day_abbr, calendar.day_name)
        MONTHS = list(zip(calendar.month_abbr, calendar.month_name))[1:]

    parsed_date = parse(date, parserinfo=LocaleParserInfo())
    if old_locale:
        locale.setlocale(locale.LC_ALL, old_locale)
    else:
        locale.setlocale(locale.LC_ALL, '')
    return parsed_date


def get_saved_url(fname):
    url = None
    saved_url = Path(fname)
    if saved_url.exists():
        with saved_url.open(encoding='utf-8') as f:
            data = yaml.safe_load(f)
            if data:
                url = data.get('URL')
    return url


def save_url(fname, url):
    if url:
        saved_url = Path(fname)
        data = {'URL': url}
        with saved_url.open('w', encoding='utf-8') as f:
            yaml.dump(data, f)


def _get_db_instance(db: Union[str, Database]) -> MongoClient:
    """
    Retrieve the pymongo.database.Database instance.

    Parameters
    ----------
    db: str or pymongo.database.Database
        - if str an instance of pymongo.database.Database will be instantiated and returned
        - if pymongo.database.Database the db instance is returned

    Returns
    -------
    pymongo.database.Database
    """
    if isinstance(db, str):
        db_name = parse_uri(db).get('database')
        if db_name is None:
            # TODO: Improve validation message
            raise ValueError("Invalid db: Could not extract database from uri: %s", db)
        db = MongoClient(db)[db_name]
    return db


def _collection_exists(db: Database, col_name: str) -> bool:
    try:
        db.validate_collection(col_name)
        return True
    except pymongo.errors.OperationFailure:
        return False


def _handle_exists_collection(name: str, exists: Optional[str], db: Database) -> None:
    """
    Handles the `if_exists` argument of `to_mongo`.

    Parameters
    ----------
    if_exists: str
        Can be one of 'fail', 'replace', 'append'
            - fail: A ValueError is raised
            - replace: Collection is deleted before inserting new documents
            - append: Documents are appended to existing collection
    """

    if exists == "fail":
        if _collection_exists(db, name):
            raise ValueError(f"Collection '{name}' already exists.")
        return

    if exists == "replace":
        if _collection_exists(db, name):
            db[name].drop()
        return

    if exists == "append":
        return

    raise ValueError(f"'{exists}' is not valid for if_exists")


def _split_in_chunks(lst: Sequence[Any], chunksize: int) -> Iterator[Sequence[Any]]:
    """
    Splits a list in chunks based on provided chunk size.

    Parameters
    ----------
    lst: list
        The list to split in chunks

    Returns
    -------
    result: generator
    A generator with the chunks
    """
    for i in range(0, len(lst), chunksize):
        yield lst[i:i + chunksize]


def _validate_chunksize(chunksize: int) -> None:
    """
    Raises the proper exception if chunksize is not valid.

    Parameters
    ----------
    chunksize: int
    The chunksize to validate.
    """
    if not isinstance(chunksize, int):
        raise TypeError("Invalid chunksize: Must be an int")
    if not chunksize > 0:
        raise ValueError("Invalid chunksize: Must be > 0")


unrestricted_page_routes = {'/login'}


class AuthMiddleware(BaseHTTPMiddleware):
    """This middleware restricts access to all NiceGUI pages.

    It redirects the user to the login page if they are not authenticated.
    """

    async def dispatch(self, request: Request, call_next):
        if not app.storage.user.get('authenticated', False):
            if not request.url.path.startswith('/_nicegui') and request.url.path not in unrestricted_page_routes:
                return RedirectResponse(f'/login?redirect_to={request.url.path}')
        return await call_next(request)
