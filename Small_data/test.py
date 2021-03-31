import pandas as pd
import matplotlib as mp

df_excel = pd.load_excel("sample_data5.xlsx")

df_excel["ID"] = pd.to_numeric(df_excel["ID"])

df_excel.plot.scatter(x="ID", y="Address")