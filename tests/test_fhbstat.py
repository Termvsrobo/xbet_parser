from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup


def test_page():
    file_path = Path(__file__).parent / Path('data') / Path('FHB_ Футбол Исход.html')
    assert file_path.exists()
    content = file_path.read_text()
    soup = BeautifulSoup(content, 'html.parser')
    assert soup.table
    df = pd.read_html(str(soup.table))
    assert not df[0].empty
