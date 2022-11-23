import pandas as pd

from classes.classDataLoader import DataLoader


class ETL:
    def __init__(self, fp):
        self.__filepath = fp
        self.dataframes = {"sales_codes": pd.DataFrame(), "vehicle_hash": pd.DataFrame()}
        self.df = None

    def run(self):
        self.load()
        self.drop()
        self.merge()
        # self.filter()

    def load(self):
        self.dataframes = DataLoader(self.__filepath).load()

    def drop(self):
        self.dataframes["sales_codes"].drop(columns="Unnamed: 0", inplace=True)
        self.dataframes["vehicle_hash"].drop(columns=["record_source", "load_ts"], inplace=True)
        self.dataframes["engines"].drop(columns=["Code Group Name En ",
                                                 "Code Group Id",
                                                 "Code Group Name De ",
                                                 "Folder Name",
                                                 "Code Description De"], inplace=True
                                        )

    def merge(self):
        df1 = self.dataframes["sales_codes"]
        merged = self.dataframes["sales_codes"].merge(right=self.dataframes["vehicle_hash"], on="h_vehicle_hash")
        self.df = merged.drop(columns=["h_vehicle_hash", "Unnamed: 0"])
