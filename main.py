from classes.classETL import ETL

etl = ETL(
    "/Users/tjarkgerken/Library/CloudStorage/OneDrive-Personal/Dokumente/WiInf-Studium/python101_praxisprojekt/data"
    "/vehicle_data.xlsx")

etl.run()
print(etl.df.head())
