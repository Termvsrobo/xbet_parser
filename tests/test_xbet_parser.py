from threading import Event

import httpx
import pytest

from parsers.xlite import XLiteParser


@pytest.mark.asyncio
async def test_parser():
    xlite_parser = XLiteParser(is_running=Event())
    test_page_url = 'https://1xlite-93399.world/ru/line/football/118593-uefa-europa-league/276945091-midtjylland-sturm-graz'
    df_data_dict = xlite_parser._parse(test_page_url)

    assert df_data_dict


@pytest.mark.asyncio
async def test_parser_json():
    async with httpx.AsyncClient() as client:
        response = await client.get(
            'https://1xlite-93399.world/service-api/LineFeed/GetGameZip?id=277966587&isSubGames=true&GroupEvents=true&countevents=250&grMode=4&topGroups=&country=1&marketType=1&isNewBuilder=true'
        )
        data = response.json()
        assert data


@pytest.mark.parametrize(
    'url,result',
    [
        (
            'https://1xlite-93399.world/ru/line/football/119237-england-league-cup/277966587-newcastle-united-bradford-city',
            '277966587'
        ),
        (
            'https://1xlite-93399.world/ru/line/football/119237-england-league-cup/xxxxxxxxx-newcastle-united-bradford-city',
            None
        ),
    ]
)
def test_get_page_id(url, result):
    page_id = XLiteParser.get_page_id(url)
    assert page_id == result
