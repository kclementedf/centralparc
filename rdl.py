#%%
import os

import pandas as pd

from icarus.io.rdl.api import RDLApi

limit_scope_to = None

limit_scope_to = [
"ANPA",
"BVER",
"JON2",
"JONC",
"ROUS",
"CY1X",
"FIEN",
"MA2X",
"TL1X",
"PEZX",
"CHPI",
"TL1X",
"MA24",
"CY1X",
"CY1X",
"BRIA",
"COLB",
"NITR",
"JAVI",
"LEMO",
"SALL",
"COTX",
"VARA",
]

data_folder_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\01.Snapshot"
export_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\03.Bluepoint import data"
objet_type_to_bluepoint_config_file = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\03 - Paramètrage\parametrage_objets.xlsx"

proxies = {
    "http":"frp-wsa-01.eu.edfencorp.net:8080", 
    "https": "frp-wsa-01.eu.edfencorp.net:8080"
    }

rdl_api = RDLApi(
    # proxies=proxies
    )

df = rdl_api.business_metadata(
    "v_wdm_eqpt_wt", 
    filters="iso_country_code",
     value="FR",
    output_type="parquet", 
    as_df=True
    )


df = df[df["project_type"]=="Wind Onshore"]

model_df = df[["model_name", "version_name", "rotor_diameter", "active_max_nominal_power_mw", "mw_installed", "manufacturer_name"]].drop_duplicates(subset=["model_name", "version_name"])
model_df["Model"] = model_df["model_name"]
model_df.loc[~model_df["version_name"].isna(), "Model"] = model_df["model_name"] + " - " + model_df["version_name"]
model_df["Manufacturer"] = model_df["manufacturer_name"]
model_df["Diamètre"] = model_df["rotor_diameter"]
model_df["Puissance nominale"] = model_df["active_max_nominal_power_mw"]
model_df["Puissance installée"] = model_df["mw_installed"]
model_df["Component Type"] = "Eolienne"
model_df["Plant ID"] = "ANPA"
model_df = model_df.dropna(subset=["Model"])
model_df[["Plant ID", "Component Type", "Model", "Manufacturer"]].to_csv(
    os.path.join(export_path, "component_models.csv"),
    sep=";", 
    index=False,
    date_format="%d/%m/%Y"
)

#%%

import_component_df = df.copy()
import_component_df["Model"] = import_component_df["model_name"]
import_component_df["Component Type"] = "Eolienne"
import_component_df["Component ID"] = import_component_df["business_eqpt_code"]
import_component_df["Plant ID"] = import_component_df["project_code"]
import_component_df.loc[~import_component_df["version_name"].isna(), "Model"] = import_component_df["model_name"] + " - " + import_component_df["version_name"]
import_component_df["Quantity"] = 1
import_component_df["Date Installed"] = pd.to_datetime(import_component_df["wt_commissioning_date"], dayfirst=True)
import_component_df["Serial Number"] = import_component_df["asset_serial_no"].fillna("")
import_component_df["Short Name"] = import_component_df["business_eqpt_code"]

import_component_df[["Model", "Component Type", "Component ID", "Plant ID", "Quantity", "Date Installed", "Serial Number", "Short Name"]].to_csv(
    os.path.join(export_path, "components.csv"),
    sep=";", 
    index=False,
    date_format="%d/%m/%Y"
)

#%%
production_budget_df = pd.read_excel(
    r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\01.Snapshot\budget_production_rdl_tableau_2023.xlsx"
).ffill()
production_budget_df.columns = ["Plant ID", "Exceedance Probability", "Forecast Type", "January",	"February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
production_budget_df["Scenario"] = "Management Case" # Choix par défaut, à ajuster
production_budget_df["Source"] = "RDL 2023"
production_budget_df = production_budget_df[production_budget_df["Exceedance Probability"].isin(["P50", "P75", "P90"])]
production_budget_df["Forecast Type"] = production_budget_df["Forecast Type"].replace({
    "Production": "Generation"
})
production_budget_df.iloc[:, 3:15] = (production_budget_df.iloc[:, 3:15].astype(float)/1000).round(5) # conversion en kWh

if limit_scope_to:
    production_budget_df = production_budget_df[production_budget_df["Plant ID"].str[:4].isin(limit_scope_to)]



production_budget_df.to_csv(
    os.path.join(export_path, "forecast.csv"),
    sep=";", 
    index=False,
    date_format="%d/%m/%Y"
)