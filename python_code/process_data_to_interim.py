import pandas as pd
import numpy as np
import os

from sklearn.ensemble import IsolationForest
from urllib.parse import urlparse

URL_TRIPADVISOR = 'https://www.tripadvisor.com'
URL_EMPTY_STRING = 'https://www.this_is_an_empty_string.com'
SECONDS_TO_DAYS = 1/60/60/24


def first_data_process(data):
    """
    This function process the data by getting the sessions into the trip
    advisor link.

    Parameters
    ----------
    data: pd.DataFrame
        Dataframe of the original raw data.

    Returns
    -------
    df: pd.DataFrame
        Dataframe that contains reduced and processed information.
    """
    df = data.copy()  # We copy so that we don't manipulate the real data.

    # Let's sort values using the even time stamp.
    df = df.sort_values('eventtimestamp')

    # We modify the empty strings into a URL_EMPTY_STRING constant for parsing
    # purposes.
    df.loc[df.referrerurl == '', 'referrerurl'] = URL_EMPTY_STRING

    # Let's identify which links contain tripadvisor information.
    df.loc[df.targeturl.str.contains(
        URL_TRIPADVISOR), 'trip_advisor_presence'] = 1
    # Now we identify which not (the complement).
    df.trip_advisor_presence = df.trip_advisor_presence.fillna(0)

    # We are skipping those values that from a same url goes to the same url
    df = df[
        ~(
            df.targeturl.str.contains(URL_TRIPADVISOR) &
            df.referrerurl.str.contains(URL_TRIPADVISOR)
        )
    ]

    # Now we care about the distinct chain of events that lead to each
    # trip_advisor link. We can consider those as "sessions" with the
    # definiton that a session is a "series" events that ultimately lead to a
    # new "TripAdvisor" url link.
    sessions = []
    index_val_pos_keep = 0
    for session_id, val in enumerate(df[df.trip_advisor_presence == 1].index):
        session_id = f"_{session_id}"
        index_val_pos = df.index.get_loc(val) + 1
        # We get a temporal dataframe that is selected from the
        # index_val_pos_keep to index_val_pos, that is to say from the start
        # of a session into the end of the session (arriving to trip advisor).
        temporal_df = df.iloc[index_val_pos_keep: index_val_pos]
        # Now we assign the corresponding sessionid (userid+session_id number).
        temporal_df['sessionid'] = temporal_df['userid'] + session_id

        if temporal_df.shape[0] == 1:
            # Sometimes the session has just 1 link, if it was an empty string
            # it doesn't help us to the analysis. Let's keep the ones that have
            # something different than an empty string.
            temporal_df = temporal_df[~(
                    temporal_df.targeturl.str.contains(URL_TRIPADVISOR) &
                    temporal_df.referrerurl.str.contains(URL_EMPTY_STRING)
            )]
        # Now we save the session into a list.
        if temporal_df.shape[0] > 0:
            sessions.append(temporal_df)
        # We need to update from which index is the new session going to start.
        index_val_pos_keep = index_val_pos
    # After iterations are done we actually removed what happened after the
    # last TripAdvisor url. This information is not relevant for us.
    if sessions:
        # If there is a list of dataframes to concatenate, then we do the
        # concatenation, if there is not, something went wrong.
        df_sessions = pd.concat(sessions).drop('userid', axis=1)
        # If the df_sessions has a lenght greater than one, we keep it.
        if df_sessions.shape[0] > 1:
            return df_sessions
        df_sessions['sessionid'] = 'something wrong'
        return df_sessions
    df['sessionid'] = 'something wrong'
    df = df.drop('userid', axis=1)
    return df


def identify_outliers(data):
    """ This function identifies outliers on a user by analyzing a sample of
    the user journey and then figuring out if there was enough time to consider
    (or not) a subsession: A subsession is defined as a time clsoseness between
    events (determined by the behavior of an Isolation forest.)

    Parameters
    ----------
    data: pd.DataFrame
        Data already processed with the corresponding sessions.

    Returns
    -------
    df: pd.DataFrame
        Data that has additional information, with respect if there are
        any subsesisons in the process.



    """
    df = data.copy()
    # We get the information between url's to see how many time is taken
    # between ordered registers.
    df['click_seconds'] = (
                df.eventtimestamp - df.eventtimestamp.shift(1)).fillna(0)
    df['click_days'] = df['click_seconds'] * SECONDS_TO_DAYS

    # Each user should have their own outlier behaviour, these are going
    if df.shape[0]:
        X = df['click_days']
        try:
            random_seed = 0
            # Train an isolation forest
            clf = IsolationForest(
                random_state=random_seed,
                n_estimators=100
            ).fit(
                # Here we select a subsample of the data, which is the 10% of
                # the user behaviour.
                X.dropna().sample(
                    frac=0.1,
                    random_state=random_seed)
                .to_numpy().reshape(-1, 1)
            )
            # We now predict for all the users value
            y = clf.predict(X.fillna(0).to_numpy().reshape(-1, 1))
        except ValueError:
            y = np.array([0] * df.shape[0])
        # We detect the identified outliers.
        df['outliers'] = y != 1
        df['outliers'] = df['outliers'].astype('int')
    else:
        df['outliers'] = 0
    # Now we make an id for each subsession.
    cumsum_val = df.outliers.cumsum()
    df['subsessionid'] = df.sessionid + '__' + cumsum_val.astype('str')
    df['subsessionid_nb'] = cumsum_val.astype('int')
    return df


def get_grouped_subsessionid_list(data, col1='referrerurl', col2='targeturl'):
    """
    This is a helper function that takes two columns of a data and gets a list
    of lists.

    Parameters
    ----------
    data: pd.DataFrame
        Processed dataframe.
    col1: str
        String that represents a column in the data frame to join with another
        column, flatten it and return the list.
    col2: str
        String that represents a column in the data frame to join with the col1
        column.

    Returns
    -------
    list_of_columns: list
        Flattened list of lists of the following form:
        [data_col1_1, data_col2_1, data_col1_2, data_col1_2, data_col2_2, ...]
    """
    df = data.copy()
    # DataFrame must contain click_seconds column.
    condition = df.click_seconds == 0
    # We keep the first click_seconds, but the additional we remove them:
    # An operation done in the same second might be just an automatic redirect.
    condition.loc[condition.idxmax()] = False
    df = df[~condition]
    # We get and flatten the results as a list.
    list_of_columns = df[[col1, col2]].to_numpy().flatten().tolist()
    return list_of_columns


def process_file(file_path):
    """ Main function that processes the file in a desired path, applying the
    first_data_process, identify_outliers and get_grouped_subsesisonid_list
    functions into the data, and other additional operations as well.

    Parameters
    ----------
    file_path: str
        Path where the original file is.

    Returns
    -------
    final_processed_data: pd.DataFrame
        Reduced DataFrame with significant lower amout of records because they
        are all compacted into lists, considering only the last subsession of
        a session.
    """
    data = pd.read_parquet(file_path)
    data_by_user = data.groupby('userid').apply(
        first_data_process
    ).reset_index()
    # We delete this from memory.
    del data

    # Let's not care about the 'something wrong': no trip_advisor_presence
    data_by_user = data_by_user[
        data_by_user.sessionid != 'something wrong'
    ].reset_index(drop=True)

    # Now we should parse the urls.
    url_parsed_referrer = data_by_user.referrerurl.apply(urlparse)
    url_parsed_target = data_by_user.targeturl.apply(urlparse)

    data_by_user['referrerurl_netloc'] = url_parsed_referrer.apply(
        lambda x: x.netloc
    )
    data_by_user['referrerurl_query'] = url_parsed_referrer.apply(
        lambda x: x.query
    )
    data_by_user['referrerurl_path'] = url_parsed_referrer.apply(
        lambda x: x.path
    )
    data_by_user['targeturl_netloc'] = url_parsed_target.apply(
        lambda x: x.netloc
    )
    data_by_user['targeturl_query'] = url_parsed_target.apply(
        lambda x: x.query
    )
    data_by_user['targeturl_path'] = url_parsed_target.apply(
        lambda x: x.path
    )
    # Let's get the outliers:
    data_outliers = data_by_user.groupby('userid').apply(
        identify_outliers
    ).drop(['userid', 'level_1'], axis=1).reset_index()

    # Let's also free memory space:
    del data_by_user

    # For each session, we will get the last subsession_id
    last_subsession = data_outliers.groupby('sessionid').subsessionid.last()
    outliers_last_subsession = data_outliers[
        data_outliers.subsessionid.isin(last_subsession)
    ]
    # Get the links as a flattened list to determine the order of events
    ordered_url_list = outliers_last_subsession.groupby('subsessionid').apply(
        get_grouped_subsessionid_list, col1='referrerurl', col2='targeturl'
    )
    # Get the links as a flattened list to determine the order of events but
    # for the principal link
    ordered_urlnetloc_list = outliers_last_subsession\
        .groupby('subsessionid')\
        .apply(
            get_grouped_subsessionid_list,
            col1='referrerurl_netloc',
            col2='targeturl_netloc'
        )
    # Now we compute some additional information such as the duration or
    # platforms used.
    final_processed_data = outliers_last_subsession\
        .groupby('subsessionid')\
        .agg(
            subsession_duration=(
                'eventtimestamp', lambda x: np.max(x) - np.min(x)
            ),
            platforms_used=(
                'platform', lambda x: x.unique().tolist()
            ),
        )
    final_processed_data['url_link_list'] = ordered_url_list
    final_processed_data['urlloc_link_list'] = ordered_urlnetloc_list
    return final_processed_data


if __name__ == '__main__':
    file_path = os.path.dirname(os.path.abspath(__file__))
    general_path = os.path.join(file_path, '..', )
    data_path_raw = os.path.join(general_path, 'data', 'raw')
    data_path_interim = os.path.join(general_path, 'data', 'interim')
    # Get the files in raw_data:
    files = [
        os.path.join(data_path_raw, doc)
        for doc in os.listdir(data_path_raw) if doc.endswith('parquet')
    ]
    # Process the files
    for file in files:
        info = process_file(file)
        data_file_interim = os.path.join(
            data_path_interim,
            file.split('/')[-1]
        )
        # Save file into interim folder.
        info.to_parquet(data_file_interim)
        print(f'Info saved into {data_file_interim}')
