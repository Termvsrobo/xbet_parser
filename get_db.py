import asyncio
from threading import Event

from config import settings
from parsers.fhbstat import FHBParser


async def get_db():
    is_running = Event()
    fhbstat_parser = FHBParser(is_running=is_running)
    fhbstat_parser.email = settings.TEST_FHBSTAT_USERNAME
    fhbstat_parser.password = settings.TEST_FHBSTAT_PASSWORD
    await fhbstat_parser.get_db()


if __name__ == '__main__':
    asyncio.run(get_db())
