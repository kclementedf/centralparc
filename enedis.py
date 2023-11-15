#%%
import glob
import os
import pandas as pd
from tqdm import tqdm

limit_scope_to = None

limit_scope_to = [
    "ANPA",
    "BVER",
    "JON2",
    "JONC",
    "ROUS",
    "CY11",
    "FIEN",
    "MA22",
    "TL11",
    "PEZI",
    "CHPI",
    "TL12",
    "TL13",
    "TL14",
    "TL15",
    "MA24",
    "CY12",
    "CY13",
    "BRIA",
    "COLB",
    "NITR",
    "JAVI",
    "LEMO",
    "SALL",
    "COT1",
    "COT2",
    "COT3",
    "COT4",
    "VARA",
]

# file_path = r"Z:\Business\OMEGA\Private\BUDGET FACTURATION\0-FACTURATION\0-Courbes de charges\Sol - Validées par ENEDIS\2023\06\[2023-06-01][2023-07-01][3 FRERES][TRFR-SS01-P].txt"
def read_enedis_index(file_path):
    df = pd.read_csv(
        file_path,
        sep="\s+",
        header=None,
    )

    df.index = pd.to_datetime(df[0] + " " + df[1], format="%d/%m/%Y %H:%M")
    df = df.drop(columns=[0, 1])
    time_resolution = 60 / df.shape[1]
    df.columns = list(range(0, df.shape[1]))
    df = df.reset_index().rename(columns={"index": "datetime"}).melt(
        id_vars="datetime",
        var_name="minutes",
        value_name="Value"
    )

    df["datetime"] = df["datetime"] + pd.to_timedelta(df["minutes"]*time_resolution, unit="m")
    df["Value"] = time_resolution * df["Value"].astype(float) / 60
    df["meter"] = file_path.split("[")[-1].split("]")[0]
    df["Plant ID"] = df["meter"].str.split('-').str[0]
    df = df.groupby("Plant ID").agg({"datetime": "first", "Value": "sum", "meter": "first"}).reset_index()

    return df

file_list = glob.glob(r"Z:\Business\OMEGA\Private\BUDGET FACTURATION\0-FACTURATION\0-Courbes de charges\Sol - Validées par ENEDIS\2023\**\*.txt")

df = pd.concat([
    read_enedis_index(file_path) for file_path in tqdm(file_list) if "[" in file_path
])

#%%
df["Date (start)"] = df["datetime"]
df["Meter"] = "Utility Statement"
df["Interval"] = "Monthly"

if limit_scope_to:
    df = df[df["Plant ID"].str[:4].isin(limit_scope_to)]

df.to_csv(
    r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\03.Bluepoint import data\enedis_per_meter.csv",
    sep=";", 
    index=False,
    date_format="%d/%m/%Y"
)

df.groupby(["Plant ID", "Date (start)", "Meter"]).agg({"Value": "sum", "Interval": "first"}).round(5).reset_index()[["Plant ID", "Date (start)", "Meter", "Interval", "Value"]].to_csv(
    r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\03.Bluepoint import data\enedis_per_plant.csv",
    sep=";", 
    index=False,
    date_format="%d/%m/%Y"
)