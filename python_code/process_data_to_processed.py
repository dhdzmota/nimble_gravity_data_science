import os
import pandas as pd
import numpy as np


URL_TRIPADVISOR = 'https://www.tripadvisor.com'
URL_EMPTY_STRING = 'https://www.this_is_an_empty_string.com'


def process_ta_data(data):
    """This function makes a series of distinct processes to obtain trip
    advisor relevant links. To do this, first of all the process identifies
    which referrer urls are empty and replace them with the previous targeturl.
    Then obtains the next_referrer and the previous target to identify when is
    a session beginning ('init'), beginning and ending ('init-end') in still a
    session ('during), or ending ('end').

    Parameters
    ----------
    data: pd.DataFrame
        Data that contains raw information to be processed.

    Returns
    -------
    df: pd.DataFrame
        Processed and reduced file to make the test3 analysis.

    """
    df = data.copy()
    # First we sort values
    df.sort_values('eventtimestamp', inplace=True)
    # We create a new column with the previous targeturl
    df['prev_targeturl'] = df.targeturl.shift(1)
    # Now we identify the empty referrers and replace them (prev_targeturl)
    empty_referrer_condition = df.referrerurl==''
    df.loc[empty_referrer_condition, 'referrerurl'] = \
        df.loc[empty_referrer_condition].prev_targeturl
    # Then when this is done, now we make a next_referer column.
    df['next_referrer'] = df.referrerurl.shift(-1)
    # Since we are making string comparison, null values can't exist.
    df.referrerurl = df.referrerurl.fillna(URL_EMPTY_STRING)
    df.next_referrer = df.next_referrer.fillna(URL_EMPTY_STRING)
    df.prev_targeturl = df.prev_targeturl.fillna(URL_EMPTY_STRING)

    # Possible values to identify a session:
    session_init0 = (
        (~df.referrerurl.str.contains(URL_TRIPADVISOR)) &
        (df.targeturl.str.contains(URL_TRIPADVISOR)) &
        (df.next_referrer.str.contains(URL_TRIPADVISOR))
    )
    session_init1 = (
        (~df.prev_targeturl.str.contains(URL_TRIPADVISOR)) &
        (df.referrerurl.str.contains(URL_TRIPADVISOR)) &
        (df.targeturl.str.contains(URL_TRIPADVISOR))
    )
    # Some init-end values
    session_init_end0 = (
        (~df.referrerurl.str.contains(URL_TRIPADVISOR)) &
        (df.targeturl.str.contains(URL_TRIPADVISOR)) &
        (~df.next_referrer.str.contains(URL_TRIPADVISOR))
    )
    session_init_end1 = (
        (~df.prev_targeturl.str.contains(URL_TRIPADVISOR)) &
        (df.referrerurl.str.contains(URL_TRIPADVISOR)) &
        (df.targeturl.str.contains(URL_TRIPADVISOR)) &
        (~df.next_referrer.str.contains(URL_TRIPADVISOR))
    )
    session_during = (
        (df.prev_targeturl.str.contains(URL_TRIPADVISOR)) &
        (df.referrerurl.str.contains(URL_TRIPADVISOR)) &
        (df.targeturl.str.contains(URL_TRIPADVISOR)) &
        (df.next_referrer.str.contains(URL_TRIPADVISOR))
    )
    session_end0 = (
        (df.prev_targeturl.str.contains(URL_TRIPADVISOR)) &
        (df.referrerurl.str.contains(URL_TRIPADVISOR)) &
        (df.targeturl.str.contains(URL_TRIPADVISOR)) &
        (~df.next_referrer.str.contains(URL_TRIPADVISOR))
    )
    session_end1 = (
        (df.prev_targeturl.str.contains(URL_TRIPADVISOR)) &
        (df.referrerurl.str.contains(URL_TRIPADVISOR)) &
        (~df.targeturl.str.contains(URL_TRIPADVISOR))
    )
    # Now we add the relevant information on each case.
    df.loc[session_end0, 'session_ta'] = 'end'
    df.loc[session_end1, 'session_ta'] = 'end'
    df.loc[session_during, 'session_ta'] = 'during'
    df.loc[session_init1, 'session_ta'] = 'init'
    df.loc[session_init0, 'session_ta'] = 'init'
    df.loc[session_init_end1, 'session_ta'] = 'init-end'
    df.loc[session_init_end0, 'session_ta'] = 'init-end'
    df['prev_session_ta'] = df.session_ta.shift(1)
    session_end2 = (
        (df.prev_targeturl.str.contains(URL_TRIPADVISOR)) &
        (~df.referrerurl.str.contains(URL_TRIPADVISOR)) &
        (~df.targeturl.str.contains(URL_TRIPADVISOR)) &
        (df.prev_session_ta != 'init-end')
    )
    df.loc[session_end2, 'session_ta'] = 'end'
    # We need to make sure of some distinct cases.
    session_nan = (
        (~df.referrerurl.str.contains(URL_TRIPADVISOR)) &
        (~df.targeturl.str.contains(URL_TRIPADVISOR))
    )
    df.loc[session_nan, 'session_ta'] = np.nan
    delete_columns = [
        'prev_targeturl',
        'next_referrer',
        'prev_session_ta'
    ]
    # Now we delete the helper columns
    df.drop(delete_columns, axis=1, inplace=True)
    return df


if __name__ == '__main__':
    file_path = os.path.dirname(os.path.abspath(__file__))
    general_path = os.path.join(file_path, '..', )
    data_path_raw = os.path.join(general_path, 'data', 'raw')
    data_path_processed = os.path.join(general_path, 'data', 'processed')
    data_file = os.path.join(data_path_processed, 'data.parquet')
    # Get the files in raw_data:
    files = [
        os.path.join(data_path_raw, doc)
        for doc in os.listdir(data_path_raw) if doc.endswith('parquet')
    ]
    information = []
    for file in files:
        info = pd.read_parquet(file)
        info = info\
            .groupby('userid')\
            .apply(process_ta_data)\
            .reset_index(drop=True)
        info = info[info.session_ta.notna()]
        information.append(info)
        print(f'Processed file {file}. Appending information...')
        break

    data = pd.concat(information)
    data.to_parquet(data_file)
    print(f'Info saved into {data_file}')
