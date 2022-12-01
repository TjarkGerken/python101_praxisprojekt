import pandas as pd


class DataLoader:
    def __init__(self, filepath: str, sheet_names: list[str]) -> None:
        self.__filepath = filepath
        self.__dictionary_of_dataframes = None
        self.__sheet_names = sheet_names

    def load(self) -> dict[str, pd.DataFrame]:
        self.__dictionary_of_dataframes = pd.read_excel(self.__filepath, sheet_name=self.__sheet_names)
        return self.__dictionary_of_dataframes
