from classes.classAnalyse import Analyse
from classes.classETL import ETL

etl = ETL(
    "/Users/tjarkgerken/Library/CloudStorage/OneDrive-Personal/Dokumente/WiInf-Studium/python101_praxisprojekt/data"
    "/vehicle_data.xlsx")
etl.run()

analysis = Analyse(etl.df_return, "01.01.2014", "31.12.2020", "./plots/")
analysis.dashboard()

analysis = Analyse(etl.df_return, "01.01.2017", "01.01.2021", "./plots/")
analysis.dashboard()
