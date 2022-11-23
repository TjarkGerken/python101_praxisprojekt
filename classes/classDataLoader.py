import pandas as pd


class DataLoader:
    def __init__(self, filepath) -> None:
        self.filepath = filepath
        self.dictionary_of_dataframes = "DoubleFuck"
        self.sheet_names = ["sales_codes", "vehicle_hash", "engines"]

    def load(self) -> dict[str, pd.DataFrame]:
        self.dictionary_of_dataframes = pd.read_excel(self.filepath, sheet_name=self.sheet_names)
        return self.dictionary_of_dataframes
