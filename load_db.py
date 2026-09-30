from threading import Event

from parsers.fhbstat import FHBParser


def load_table_data_db():
    is_running = Event()
    fhbstat_parser = FHBParser(is_running=is_running)
    fhbstat_parser.load_table_data_db()


if __name__ == '__main__':
    load_table_data_db()
