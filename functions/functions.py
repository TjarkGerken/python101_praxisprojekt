import datetime as dt

import numpy as np
import pandas as pd


def df_slice_timeframe(df_to_slice, start_date: str, end_date: str) -> pd.DataFrame:
    """
    :param df_to_slice: Expects a Pandas Dataframe with a column "production_date"
    :param start_date: The start date is inclusive and defines the lower threshhold. Needs to be passed in the following Format: "dd.mm.YYYY"
    :param end_date:The end date is exclusive and defines the upper threshhold. Needs to be passed in the following Format: "dd.mm.YYYY"
    :return: Returns a dataframe with all values in between the start and the end date
    """
    start_date = dt.datetime.strptime(start_date, "%d.%m.%Y")
    end_date = dt.datetime.strptime(end_date, "%d.%m.%Y")
    sliced_dataframe = df_to_slice[np.logical_and(df_to_slice["production_date"] >= start_date,
                                                  df_to_slice["production_date"] < end_date)]
    return sliced_dataframe
