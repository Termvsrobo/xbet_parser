import os
from datetime import datetime
from pathlib import Path

import pytest
from xlsxtpl.writerx import BookWriter

from parsers.fhbstat import FHBParser


def test_create_excel_template():
    pth = os.path.dirname(__file__)
    fname = os.path.join(pth, 'data/example.xlsx')
    writer = BookWriter(fname)
    writer.jinja_env.globals.update(dir=dir, getattr=getattr)

    now = datetime.now()

    person_info = {'address': u'福建行中书省福宁州傲龙山庄', 'name': u'龙傲天', 'fm': 178, 'date': now}
    person_info2 = {'address': u'Somewhere over the rainbow', 'name': u'Hello Wizard', 'fm': 156, 'date': now}
    rows = [
        ['1', '1', '1', '1', '1', '1', '1', '1',],
        ['1', '1', '1', '1', '1', '1', '1', '1',],
        ['1', '1', '1', '1', '1', '1', '1', '1',],
        ['1', '1', '1', '1', '1', '1', '1', '1',],
        ['1', '1', '1', '1', '1', '1', '1', '1',],
        ['1', '1', '1', '1', '1', '1', '1', '1',],
        ['1', '1', '1', '1', '1', '1', '1', '1',],
        ['10', '1', '1', '1', '1', '1', '1', '10',],
    ]
    person_info['rows'] = rows
    person_info2['rows'] = rows
    payload0 = {'tpl_name': 'cn', 'sheet_name': u'表',  'ctx': person_info}
    payload1 = {'tpl_name': 'en', 'sheet_name': u'form', 'ctx': person_info2}
    payload2 = {'tpl_idx': 2, 'ctx': person_info2}
    payloads = [payload0, payload1, payload2]
    writer.render_book2(payloads=payloads)
    fname = os.path.join(pth, 'result10.xlsx')
    writer.save(fname)
    payloads = [payload2, payload1, payload0]
    writer.render_book2(payloads=payloads)
    writer.render_sheet(person_info2, 'form2', 1)
    fname = os.path.join(pth, 'result11.xlsx')
    writer.save(fname)


@pytest.mark.parametrize(
    'source_filename,template_name',
    [
        ('FHB_ Футбол Исход.html', 'ШАБЛОН Эксель Футбол Исход.xlsx'),
        ('FHB_ Хоккей Исход.html', 'ШАБЛОН Эксель Хоккей Исход.xlsx'),
        ('FHB_ Футбол Тотал_2.html', 'ШАБЛОН Эксель Футбол Тотал.xlsx'),
    ]
)
def test_fill_excel_template_from_df(source_filename, template_name):
    file_path = Path(__file__).parent / Path('data') / Path(source_filename)
    assert file_path.exists()
    content = file_path.read_text()
    df = FHBParser.parse_content(content)
    head_df = FHBParser.parse_head_table(content)
    columns = list(filter(lambda x: int(x) >= 25, head_df.columns[:-1]))
    df.update(head_df.loc[:, columns])

    fname = Path(__file__).parent.parent / Path('excel_templates') / Path(template_name)
    writer = BookWriter(fname)
    writer.jinja_env.globals.update(dir=dir, getattr=getattr)

    data = dict()
    data['rows'] = df.to_dict('records')
    payload0 = {'tpl_idx': 1, 'sheet_name': 'Статистика',  'ctx': data}

    payloads = [payload0]
    writer.render_book2(payloads=payloads)
    writer.save('1.xlsx')
