import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from functions.functions import df_slice_timeframe

pd.options.mode.chained_assignment = None


class Analyse:
    def __init__(self, dataframe: pd.DataFrame(), file_save_path: str, query: int, start_date: str = None,
                 end_date: str = None) -> None:
        self.save_path = file_save_path
        self.query = query

        self.start_date = start_date
        self.end_date = end_date
        if np.logical_or(self.start_date is None, self.end_date is None):
            self.df_timeframe_adjusted = dataframe.sort_values(by="production_date")
        else:
            self.df_timeframe_adjusted = df_slice_timeframe(dataframe, self.start_date, self.end_date).sort_values(
                by="production_date")

        self.sales_by_engine = self.filter_by_engines(["OM 934", "OM 936", "OM 470", "OM 471"])
        plt.rcParams["figure.figsize"] = [8, 5]
        plt.rcParams['figure.dpi'] = 300

    def dashboard(self) -> None:
        print("---------- Dashboard ----------")
        print(
            f"The timeframe for the analysis is: "
            f"\n{self.df_timeframe_adjusted['production_date'].iloc[1].strftime('%d.%m.%Y')}"
            f" til {self.df_timeframe_adjusted['production_date'].iloc[-1].strftime('%d.%m.%Y')}")
        print("-------------------------------\n")

        if self.query == 1:
            self.query1()
        elif self.query == 2:
            self.query2()
        elif self.query == 3:
            self.query3()
        else:
            print("Not a valid query")

    def query1(self) -> None:
        print(f"The countries with the biggest sales:\n"
              f"Most Sales in:  {self.__top_three_countries()[0]}\n"
              f"Second most Sales in:  {self.__top_three_countries()[1]}\n"
              f"Third most Sales in:  {self.__top_three_countries()[2]}\n")
        print("-------------------------------\n")
        print(f"The most vehicles were sold in {self.__grouped_by_year()}\n")
        print("-------------------------------\n")

    def query2(self) -> None:
        print(f"In the specified timeframe {self.sales_by_engine} vehicles were sold with the specified engines.\n")
        print("-------------------------------\n")
        print(f"The car that was sold to New Zealand in the specified timeframe with the OM 936 Motor has the VIN:")
        print(f"{self.filter_by_country_engine('Neuseeland', 'OM 936')} \n")
        print("-------------------------------\n")

    def query3(self) -> None:
        print(f"The first vehicle sold has the VIN:")
        print(f"{self.first_vehicle()}\n")
        print("------- End of Dashboard -------")

    def __top_three_countries(self) -> list[str]:
        sales_grouped_by_year = \
            self.df_timeframe_adjusted.groupby(by="country").count().sort_values(by=["fin", "country"], ascending=False)
        data_for_plot = sales_grouped_by_year.iloc[[0, 1, 2]]
        data_for_plot.plot(use_index=True, y=["fin"], kind="bar", color="silver")
        plt.title("Sales by Country (Top Three)")
        plt.xlabel("Country")
        plt.ylabel("# of Sales")
        plt.legend().set_visible(False)
        plt.xticks(rotation=360)
        plt.savefig(self.save_path + "top_three_countries.png")
        return list(sales_grouped_by_year["fin"].index[0:3])

    def __grouped_by_year(self) -> pd.DataFrame:
        self.df_timeframe_adjusted.loc[:, "year"] = pd.DatetimeIndex(
            self.df_timeframe_adjusted.loc[:, "production_date"]).year
        df_grouped_by_year = self.df_timeframe_adjusted.groupby(by="year").count()
        df_grouped_by_year_highest = df_grouped_by_year.sort_values(by="fin")["fin"].index[-1]

        df_grouped_by_year.plot(use_index=True, y=["fin"], color="silver")
        plt.title("Sales by Year")
        plt.xlabel("Year")
        plt.legend().set_visible(False)
        plt.ylabel("# of Sales")
        plt.savefig(self.save_path + "sales_by_year.png")
        return df_grouped_by_year_highest

    def filter_by_engines(self, list_of_engines) -> pd.DataFrame:
        counted = self.df_timeframe_adjusted.groupby(by="engine").count()
        counted_filtered = counted.filter(items=list_of_engines, axis=0)

        counted_filtered.plot(use_index=True, y=["fin"], kind="bar", color="silver")
        plt.title(f"Sales by Engine. Filtered for: {list_of_engines}")
        plt.xlabel("Engine Name")
        plt.ylabel("# of Sales")
        plt.legend().set_visible(False)
        plt.xticks(rotation=360)
        plt.savefig(self.save_path + "sales_by_engines_filtered.png")
        return counted_filtered["fin"].sum()

    def first_vehicle(self) -> str:
        first_vin = self.df_timeframe_adjusted.iloc[0, :]["fin"]
        return first_vin

    def filter_by_country_engine(self, country, engine) -> str:
        filtered = self.df_timeframe_adjusted[np.logical_and(self.df_timeframe_adjusted["country"] == country,
                                                             self.df_timeframe_adjusted["engine"] == engine)]
        return filtered["fin"].item()
