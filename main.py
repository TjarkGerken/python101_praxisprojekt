from classes.classAnalyse import Analyse
from classes.classETL import ETL

wiessler_fp = "C:\\Users\\user\\Dropbox\\DHBW Stuttgart\\Informatik 1 Python\\Projektarbeit_Wiessler\\vehicle_data.xlsx"
tjark_fp = "./data/vehicle_data.xlsx"
etl = ETL(fp=wiessler_fp)
etl.run()

query1 = Analyse(dataframe=etl.df_return, start_date="01.01.2014", end_date="31.12.2020", file_save_path="./plots/",
                 query=1)
query2 = Analyse(dataframe=etl.df_return, start_date="01.01.2017", end_date="01.01.2021", file_save_path="./plots/",
                 query=2)
query3 = Analyse(dataframe=etl.df_return, file_save_path="./plots/", query=3)

print("First Query:")
query1.dashboard()
print("Second Query:")
query2.dashboard()
print("Third Query:")
query3.dashboard()
