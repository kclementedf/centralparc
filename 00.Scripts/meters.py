# %%
import os
import platform
import re
import glob

import pandas as pd

limit_scope_to = None

limit_scope_to = [
# "ANPA",
# "BVER",
# "JONC",
# "ARAM",
# "EYGU",
# "SACU", # vente au post
# "PEZI",
"BTSX",
"LBDF",
"LDTE",
"MACH",
"PLAN",
"TRAY",
"BRIA",
"NITR,"
"COLB",
"CY11",
"CY12",
"CY13",
"MA22",
"MA24",
"SUBL",
"TL11",
"TL12",
"TL13",
"TL14",
"TL15",
"VACH",
]

if platform == "win32":
    data_folder_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 03 - Réalisation - 03 - Réalisation\02.Migration données\01.Snapshot"
    export_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 03 - Réalisation - 03 - Réalisation\02.Migration données\03.Import data\01.Brut"
    objet_type_to_bluepoint_config_file = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 03 - Réalisation - 03 - Réalisation\01.Bluepoint\parametrage_objets.xlsx"

if platform == "linux":
    data_folder_path = r"/home/EDF/centralparc/01.Snapshot"
    export_path = r"//home/EDF/centralparc/03.Import data/01.Brut"
    objet_type_to_bluepoint_config_file = r"/home/EDF/centralparc/parametrage_objets.xlsx"

centrale_df = pd.read_parquet(os.path.join(data_folder_path, "centrale.parquet"))

projet_df = pd.read_parquet(os.path.join(data_folder_path, "projet.parquet"))

type_entite_element_df = pd.read_parquet(
    os.path.join(data_folder_path, "type_entite_element.parquet")
)

type_projet_df = pd.read_parquet(os.path.join(data_folder_path, "type_projet.parquet"))

# %%
projet_df = projet_df.merge(
    type_projet_df[["ID_TYPE_PROJET", "LIBELLE_TYPE_PROJET"]],
    left_on=["ID_TYPE_PROJET"],
    right_on=["ID_TYPE_PROJET"],
    how="left",
)
# %%
centrale_df = centrale_df.merge(
    projet_df[["ID_PROJET", "LIBELLE_TYPE_PROJET"]], how="left"
)

# %%
meters_df = pd.concat(
    [
        pd.read_csv(file_path, parse_dates=[0], decimal=",")
        for file_path in glob.glob(os.path.join(data_folder_path, "meters", "*.csv"))
    ]
)

meters_df = meters_df[meters_df["project"].str.len() == 4]

# %% Meters
meters_to_bluepoint = {
    "project": "Plant ID",
    "date": "Date (start)",
    "Interval": "Interval",
    "monthly_asset_contract_tba": "Availability",
    "act_energy_consumption": "Consumption",
    "act_energy_generation": "Gross Generation",
    "monthly_asset_contract_availability": "OEM Availability",
    "duration_running": "Operational Hours",
    "act_energy_iec": "Production",
    "wind_speed_avg": "Wind Speed",
    "pot_energy_iec_consolidated": "Wind Theoretical Production",
}

import_meter_df = meters_df.copy()

import_meter_df["Interval"] = "Monthly"

import_meter_df = import_meter_df[list(meters_to_bluepoint.keys())].rename(
    columns=meters_to_bluepoint
)

import_meter_df[["Availability", "OEM Availability"]] = 100*import_meter_df[["Availability", "OEM Availability"]]

import_meter_df = pd.melt(
    import_meter_df,
    id_vars=["Plant ID", "Date (start)", "Interval"],
    var_name="Meter",
    value_name="Value",
).dropna(subset=["Value"])

import_meter_df["Value"] = (
    import_meter_df["Value"].astype(float).round(5)
)

if limit_scope_to:
    import_meter_df = import_meter_df[import_meter_df["Plant ID"].str[:4].isin(limit_scope_to)]


import_meter_df.to_csv(
    os.path.join(export_path, "meters.csv"),
    sep=";",
    index=False,
    date_format="%d/%m/%Y",
)
