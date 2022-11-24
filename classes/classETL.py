import datetime as dt

import numpy as np
import pandas as pd

from classes.classDataLoader import DataLoader


class ETL:
    def __init__(self, fp):
        self.__filepath = fp
        self.__input_df = {"sales_codes": pd.DataFrame(), "vehicle_hash": pd.DataFrame()}
        self.return_df = None

    def run(self):
        self.__load()
        self.__drop()
        self.__merge()
        self.__filter()
        self.__attach_engines()

    def __load(self):
        self.__input_df = DataLoader(self.__filepath).load()

    def __drop(self):
        self.__input_df["sales_codes"].drop(columns="Unnamed: 0", inplace=True)
        self.__input_df["vehicle_hash"].drop(columns=["record_source", "load_ts"], inplace=True)
        self.__input_df["engines"].drop(columns=["Code Group Name En ",
                                                 "Code Group Id",
                                                 "Code Group Name De ",
                                                 "Folder Name",
                                                 "Code Description De"],
                                        inplace=True
                                        )

    def __merge(self):
        merged = self.__input_df["sales_codes"].merge(right=self.__input_df["vehicle_hash"], on="h_vehicle_hash")
        self.return_df = merged.drop(columns=["h_vehicle_hash", "Unnamed: 0"])

    def __filter(self):
        self.return_df["production_date"] = pd.to_datetime(self.return_df["production_date"],
                                                           format="%Y-%m-%d %H:%M:%S",
                                                           errors="coerce").dropna()
        self.return_df = self.return_df[
            np.logical_and(self.return_df["production_date"] > dt.datetime(year=2010, day=1, month=1),
                           dt.datetime.now() > self.return_df["production_date"])]
        self.return_df = self.return_df.dropna()
        self.return_df = self.return_df[self.return_df["fin"].str.len() == 17]

    def __attach_engines(self):
        def check(entry):
            for key, value in engines["Sales Code"].items():
                if value in entry:
                    return engines["Code Description En"][key]

        engines = self.__input_df["engines"].loc[:, "Sales Code":"Code Description En"].to_dict()
        self.return_df["engine"] = [check(entry) for entry in self.return_df["sales_code_array"]]
