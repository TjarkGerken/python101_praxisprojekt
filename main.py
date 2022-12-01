from classes.classAnalyse import Analyse
from classes.classETL import ETL

fp = "./data/vehicle_data.xlsx"
etl = ETL(fp=fp)
etl.run()

# query1 = Analyse(dataframe=etl.df_return, start_date="01.01.2014", end_date="31.12.2020", file_save_path="./plots/",
#          query=1)
query2 = Analyse(dataframe=etl.df_return, start_date="01.01.2017", end_date="01.01.2021", file_save_path="./plots/",
                 query=2)
# query3 = Analyse(dataframe=etl.df_return, file_save_path="./plots/", query=3)

# print("First Query:")
# query1.dashboard()
# print("Second Query:")
print(query2.filter_by_engines(["OM 934", "OM 936", "OM 470", "OM 471"]))
# print("Third Query:")
# query3.dashboard()
