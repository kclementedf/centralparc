#%%
import glob
import os
import pandas as pd
from tqdm import tqdm
from sys import platform

limit_scope_to = None

# limit_scope_to = [
#     "ANPA",
#     "BVER",
#     "JON2",
#     "JONC",
#     "ROUS",
#     "CY11",
#     "FIEN",
#     "MA22",
#     "TL11",
#     "PEZI",
#     "CHPI",
#     "TL12",
#     "TL13",
#     "TL14",
#     "TL15",
#     "MA24",
#     "CY12",
#     "CY13",
#     "BRIA",
#     "COLB",
#     "NITR",
#     "JAVI",
#     "LEMO",
#     "SALL",
#     "COT1",
#     "COT2",
#     "COT3",
#     "COT4",
#     "VARA",
# ]

if platform== "linux":
    file_folder_path = "/home/xx"

else:
    file_folder_path = r"C:\Users\kclement\EDF Renouvelables\Icarus - teams - Documents\data_hub\meter\enedis\courbes_de_charges\processed\year=2023"

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

#%% Récupération des fichier Enedis brut
# file_list = glob.glob(os.path.join(
#     file_folder_path,
#     "**",
#     "*.txt"
#     ), 
#     recursive=True
#     )


# df = pd.concat([
#     read_enedis_index(file_path) for file_path in tqdm(file_list)
# ])

#%% Fichier nettoyés perf
file_list = glob.glob(os.path.join(
    file_folder_path,
    "**",
    "*.csv"
    ), 
    recursive=True
    )

df = []
failed = []
for file_path in tqdm(file_list):
    try:
        df.append(pd.read_csv(file_path, sep=";"))
        df[-1]["Meter ID"] = os.path.basename(file_path)
    except:
        failed.append(file_path)
#%%
df = pd.concat(df)
df["ts"] = pd.to_datetime(df["ts"])
df['ts'] = df["ts"].apply(lambda x: x.replace(day=1)) # très lent
# df["ts"] = df['ts'] - pd.offsets.MonthBegin(1, normalize=True) # rapide mais juste ?
df["Date (start)"] = df["ts"].dt.date
df["Meter ID"] = df["Meter ID"].str.split("_").str[2].str.split(".").str[0]
df["Plant ID"] = df["Meter ID"].str.split('-').str[0]
df = (df
      .groupby(["Plant ID", "Date (start)"])
      .agg({"active_power_avg": "sum"})
      .reset_index()
      .rename(columns={"active_power_avg": "Value"})
)
df["Meter"] = "Utility Statement"
df["Interval"] = "Monthly"
df["Value"] = (df["Value"] / 6).round(5) 
#%%

if limit_scope_to:
    df = df[df["Plant ID"].str[:4].isin(limit_scope_to)]

#%%
df.to_excel(
    os.path.join(
        os.path.dirname(__file__),
        "enedis.xlsx"
    ),
    index=False,
    # sep=";", 
    # date_format="%d/%m/%Y"
)