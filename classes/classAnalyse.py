import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from functions.functions import df_slice_timeframe

pd.options.mode.chained_assignment = None


class Analyse:
    def __init__(self, dataframe: pd.DataFrame(), start_date: str, end_date: str, fp):
        self.df_from_data = dataframe
        self.start_date = start_date
        self.end_date = end_date
        self.save_path = fp
        self.df_timeframe_adjusted = df_slice_timeframe(self.df_from_data, self.start_date, self.end_date)
        self.sales_by_engine = self.filter_by_engines(["OM 934", "OM 936", "OM 470", "OM 471"])

    def dashboard(self):
        print("---------- Dashboard ----------")
        print(f"The timeframe for the analysis is: \n{self.start_date} til {self.end_date}")

        print("-------------------------------\n")
        print(f"The countries with the biggest sales:\n"
              f"Most Sales in:  {self.__top_three_countries()[0]}\n"
              f"Second most Sales in:  {self.__top_three_countries()[1]}\n"
              f"Third most Sales in:  {self.__top_three_countries()[2]}\n")
        print("-------------------------------\n")
        print(f"The most vehicles were sold in {self.__most_sold_by_year()}\n")
        print("-------------------------------\n")
        print(f"In the specified timeframe {self.sales_by_engine} vehicles were sold with the specified engines.\n")
        print("-------------------------------\n")
        print(f"The car that was sold to New Zealand in the specified timeframe with the OM 936 Motor has the VIN:")
        print(f"{self.filter_by_country_engine('Neuseeland', 'OM 936')} \n")

        print("-------------------------------\n")
        print(f"The first vehicle sold has the VIN:")
        print(f"{self.first_vehicle()}\n")
        print("------- End of Dashboard -------")

    def __top_three_countries(self):
        sales_grouped_by_year = \
            self.df_timeframe_adjusted.groupby(by="country").count().sort_values(by="fin", ascending=False)

        plt.figure(1, dpi=500)
        plt.bar(data=sales_grouped_by_year, x=sales_grouped_by_year.index, height=sales_grouped_by_year["fin"])
        plt.savefig(self.save_path)
        return list(sales_grouped_by_year["fin"].index[0:3])

    def __most_sold_by_year(self):
        self.df_timeframe_adjusted.loc[:, "year"] = pd.DatetimeIndex(
            self.df_timeframe_adjusted.loc[:, "production_date"]).year
        sales_grouped_by_year = self.df_timeframe_adjusted.groupby(by="year").count().sort_values(by="fin")["fin"]
        return sales_grouped_by_year.index[-1]

    def filter_by_engines(self, list_of_engines):
        counted = self.df_timeframe_adjusted.groupby(by="engine").count()
        counted_filtered = counted.filter(items=list_of_engines, axis=0)
        return counted_filtered["fin"].sum()

    def first_vehicle(self):
        sorted_values = self.df_timeframe_adjusted.sort_values(by="production_date", ascending=True).iloc[0, :]["fin"]
        return sorted_values

    def filter_by_country_engine(self, country, engine):
        filtered = self.df_timeframe_adjusted[np.logical_and(self.df_timeframe_adjusted["country"] == country,
                                                             self.df_timeframe_adjusted["engine"] == engine)]
        return filtered["fin"].item()


"""
Done:
- Welche FIN hat das zeitlich erste verkaufte Fahrzeug.
- Welches sind die top drei Länder, in die wir zwischen 01.01.2014 und 31.12.2020 am meisten Fahrzeuge verkauft haben.x
- In welchem dieser Jahre haben wir insgesamt am meisten Fahrzeuge verkauft? x
- Wie viele Fahrzeuge wurden zwischen 01.01.2017 und 01.01.2021 mit OM 934, OM 936, OM 470 und OM 471 Motoren verkauft.
- Welche Fahrzeuge (FIN) wurden zwischen 01.01.2017 und 01.01.2021 und mit OM 936 Motor nach Neuseeland verkauft.
"""
