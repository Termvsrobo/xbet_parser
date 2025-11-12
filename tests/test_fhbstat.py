from pathlib import Path

from parsers.fhbstat import FHBParser


def test_page():
    file_path = Path(__file__).parent / Path('data') / Path('FHB_ Футбол Исход.html')
    assert file_path.exists()
    content = file_path.read_text()
    df = FHBParser.parse_content(content)
    head_df = FHBParser.parse_head_table(content)
    assert not df.empty
    assert not head_df.empty
