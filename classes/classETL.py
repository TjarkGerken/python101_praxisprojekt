import datetime as dt

import pandas as pd

from classes.classDataLoader import DataLoader


class ETL:
    def __init__(self, fp: str, etl_report: bool = False):
        self.__filepath = fp
        self.__input_df = {"sales_codes": pd.DataFrame(), "vehicle_hash": pd.DataFrame()}
        self.df_return = None
        self.__report = etl_report

    def run(self):
        self.__load()
        self.__drop_columns()
        self.__merge()
        self.__filter()
        self.__attach_engines_my_solution()
        self.__save()

    def __load(self):
        self.__input_df = DataLoader(self.__filepath, ["sales_codes", "vehicle_hash", "engines"]).load()

    def __drop_columns(self):
        self.__input_df["sales_codes"].drop(columns="Unnamed: 0", inplace=True)
        self.__input_df["vehicle_hash"].drop(columns=["record_source", "load_ts", "Unnamed: 0"], inplace=True)
        self.__input_df["engines"].drop(columns=["Code Group Name En ",
                                                 "Code Group Id",
                                                 "Code Group Name De ",
                                                 "Folder Name",
                                                 "Code Description De"],
                                        inplace=True
                                        )

    def __merge(self):
        merged = self.__input_df["sales_codes"].merge(right=self.__input_df["vehicle_hash"], on="h_vehicle_hash")
        self.df_return = merged.drop(columns=["h_vehicle_hash"])

    def __filter(self):
        length_before_removal = len(self.df_return)
        print("------------- Report -------------")
        self.__filter_fin()
        self.__filter_date()
        self.__dropna()
        self.clean_sales_array()
        print("--------- Report Summary ---------")
        if self.__report:
            print(f"Removed entries: {(length_before_removal - len(self.df_return))}")
            print(f"The final amount of entries is: {len(self.df_return)}")
        print("--------- End of Report --------- \n")

    def __filter_date(self):
        length_before_removal = len(self.df_return)
        self.df_return["production_date"] = pd.to_datetime(self.df_return["production_date"],
                                                           format="%Y-%m-%d %H:%M:%S",
                                                           errors="coerce").dropna()
        self.df_return = self.df_return.loc[
            (dt.datetime.now() > self.df_return["production_date"]) &
            (self.df_return["production_date"] > dt.datetime(year=2010, day=1, month=1))]
        if self.__report:
            print(f"Entries removed with unreasonable dates: {(length_before_removal - len(self.df_return))}")

    def __filter_fin(self):
        length_before_removal = len(self.df_return)
        self.df_return = self.df_return[self.df_return["fin"].str.len() == 17]
        if self.__report:
            print(f"Removed {(length_before_removal - len(self.df_return))} Entries with a wrong fin length.")

    def __dropna(self):
        length_before_removal = len(self.df_return)
        self.df_return = self.df_return.dropna()
        if self.__report:
            print(f"Entries removed without Values: {(length_before_removal - len(self.df_return))}")

    def __attach_engines(self):
        engine_column = []
        engines = self.__input_df["engines"][["Sales Code", "Code Description En"]]. \
            set_index("Sales Code"). \
            to_dict()["Code Description En"]
        for sc_array in self.df_return["sales_code_array"]:
            engine = engines[list(set(engines.keys()) & set(sc_array.split(", ")))[0]]
            engine_column.append(engine)
        self.df_return["engine"] = engine_column

    def clean_sales_array(self):
        """The function checks if there is more than one engine sales code in the column sales code array.
            If this is the cases it drops the second one and the first will get treated as the right one.
            """
        new_sales_array = []
        engine_codes = ["M0I", "M0J", "Z5A", "Z5B", "Z5C", "Z5D", "Z5E", "Z5F", "Z5L"]
        duplicates = 0
        for entry in self.df_return["sales_code_array"]:
            amount_of_engines = 0
            list_of_sales_codes = entry.split(", ")
            filtered_list = []
            for sales_code in list_of_sales_codes:
                if sales_code in engine_codes:
                    if amount_of_engines >= 1:
                        amount_of_engines += 1
                        duplicates += 1
                    else:
                        amount_of_engines += 1
                        filtered_list.append(sales_code)
                else:
                    filtered_list.append(sales_code)

            filtered_row = None

            for item in filtered_list:
                if filtered_row is None:
                    filtered_row = item
                else:
                    filtered_row = filtered_row + ", " + item
            new_sales_array.append(filtered_row)
        self.df_return["sales_code_array"] = new_sales_array
        if self.__report:
            print(f"Cars with at least 2 Engines: {duplicates}.")

    def __attach_engines_my_solution(self):
        def check(entry):
            for key, value in dictionary_engines_codes["Sales Code"].items():
                if value in entry:
                    return dictionary_engines_codes["Code Description En"][key]

        dictionary_engines_codes = self.__input_df["engines"].loc[:, "Sales Code":"Code Description En"].to_dict()
        self.df_return["engine"] = [check(entry) for entry in self.df_return["sales_code_array"]]

    def __save(self):
        fp = "./data/enhanced_vehicle_data.xlsx"
        self.df_return[["production_date", "country", "sales_code_array", "fin", "engine"]].to_excel(fp)
