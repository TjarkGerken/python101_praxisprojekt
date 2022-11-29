import datetime as dt

import numpy as np
import pandas as pd

from classes.classDataLoader import DataLoader


class ETL:
    def __init__(self, fp):
        self.__filepath = fp
        self.__input_df = {"sales_codes": pd.DataFrame(), "vehicle_hash": pd.DataFrame()}
        self.df_return = None

    def run(self):
        self.__load()
        self.__drop()
        self.__merge()
        self.__filter()
        self.__attach_engines()
        self.__save()

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
        self.df_return = merged.drop(columns=["h_vehicle_hash", "Unnamed: 0"])

    def __filter(self):
        self.df_return["production_date"] = pd.to_datetime(self.df_return["production_date"],
                                                           format="%Y-%m-%d %H:%M:%S",
                                                           errors="coerce").dropna()
        self.df_return = self.df_return[
            np.logical_and(self.df_return["production_date"] > dt.datetime(year=2010, day=1, month=1),
                           dt.datetime.now() > self.df_return["production_date"])]
        self.df_return = self.df_return[self.df_return["fin"].str.len() == 17]
        self.df_return = self.df_return.dropna()

    def __attach_engines(self):
        def check(entry):
            for key, value in engines["Sales Code"].items():
                if value in entry:
                    return engines["Code Description En"][key]

        engines = self.__input_df["engines"].loc[:, "Sales Code":"Code Description En"].to_dict()
        self.df_return["engine"] = [check(entry) for entry in self.df_return["sales_code_array"]]

    def __save(self):
        fp = "./data/enhanced_vehicle_data.xlsx"
        self.df_return[["production_date", "country", "sales_code_array", "fin"]].to_excel(fp)
