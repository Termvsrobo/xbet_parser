from datetime import datetime
from io import StringIO

import pandas as pd
from pandas.testing import assert_frame_equal


def test_round():
    df = pd.DataFrame({
        'Название': ['n1', 'n2', 'n3'],
        'Value': [1.285, 2.334, 2.555],
        'Rate': [4, 1.001, 1.005]
    })
    df.iloc[:, 1:] = (df.iloc[:, 1:] + pow(10, -4)).round(2)
    assert_frame_equal(
        df,
        pd.DataFrame({
            'Название': ['n1', 'n2', 'n3'],
            'Value': [1.29, 2.33, 2.56],
            'Rate': [4, 1.00, 1.01]
        })
    )


def test_save_json_date():
    df = pd.DataFrame({
        'Название': ['n1', 'n2', 'n3'],
        'Value': [1.29, 2.33, 2.56],
        'Rate': [4, 1.00, 1.01],
        'Дата': [datetime(2025, 3, 4, 15, 45), datetime(2025, 3, 5, 15, 45), datetime(2025, 3, 6, 15, 45)]
    })

    json_io = StringIO()
    df.to_json(json_io, date_unit='s', date_format='iso')
    new_df = pd.read_json(json_io.getvalue(), date_unit='s')
    new_df['Дата'] = new_df['Дата'].astype('datetime64[ns]')

    assert_frame_equal(df, new_df)
