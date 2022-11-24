import pandas as pd


class DataLoader:
    def __init__(self, filepath) -> None:
        self.__filepath = filepath
        self.__dictionary_of_dataframes = None
        self.__sheet_names = ["sales_codes", "vehicle_hash", "engines"]

    def load(self) -> dict[str, pd.DataFrame]:
        self.dictionary_of_dataframes = pd.read_excel(self.__filepath, sheet_name=self.__sheet_names)
        return self.dictionary_of_dataframes
