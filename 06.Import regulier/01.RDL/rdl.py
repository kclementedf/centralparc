#%%
import os
from sys import platform

import pandas as pd
import tqdm
import numpy as np

from icarus.io.rdl.api import RDLApi, StaticDataView
from icarus import CORPORATE_PROXIES

start_date = "2023-01-01"
end_date = "2023-11-30"

limit_scope_to = None

# limit_scope_to = [
# "ANPA",
# "BVER",
# "JON2",
# "JONC",
# "ROUS",
# "CY1X",
# "FIEN",
# "MA2X",
# "TL1X",
# "PEZX",
# "CHPI",
# "TL1X",
# "MA24",
# "CY1X",
# "CY1X",
# "BRIA",
# "COLB",
# "NITR",
# "JAVI",
# "LEMO",
# "SALL",
# "COTX",
# "VARA",
# ]

if platform == "windows":
    data_folder_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\01.Snapshot"
    export_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\03.Bluepoint import data"
    objet_type_to_bluepoint_config_file = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\03 - Paramètrage\parametrage_objets.xlsx"

if platform == "linux":
    data_folder_path = r"/home/EDF/centralparc/01.Snapshot"
    export_path = r"//home/EDF/centralparc/03.Import data/01.Brut"
    objet_type_to_bluepoint_config_file = r"/home/EDF/centralparc/parametrage_objets.xlsx"

rdl_api = RDLApi(
    # proxies=CORPORATE_PROXIES.FR,
    )

project_list = rdl_api.business_metadata(StaticDataView.PROJECT, filters="iso_country_code", value="FR")
project_list = [project for project in project_list if project["project_type"]=="Wind Onshore"]

if limit_scope_to:
    project_list = [project for project in project_list if project["project_code"] in limit_scope_to]

#%% Indicateurs mensuels
tech_monthly_attr = rdl_api.technical_views(
                "engine_attributes",
            filters=[
            {
            "column": "endpoint",
            "value": "tech_monthly_agg"
            },
            {
            "column": "mode",
            "value": "reclassified"
            }
            ] 
            )

tech_monthly_device = []
for project in tqdm.tqdm(project_list):
    try:
        tech_monthly_device.append(rdl_api.technical_monthly(
            project["project_code"],
            start_date,
            end_date,
            attributes=[attr["code"] for attr in tech_monthly_attr],
            mode="reclassified",
            output_type="parquet",
            as_df=True
            ))
    except:
        pass
tech_monthly_device = pd.concat(tech_monthly_device)

tech_monthly_device = tech_monthly_device.replace("nan", np.nan)
tech_monthly_device = tech_monthly_device.apply(lambda x: pd.to_numeric(x, errors="ignore"))
tech_monthly_device["date"] = pd.to_datetime(tech_monthly_device["date"], format="%Y-%m", dayfirst=True)
tech_monthly_device = tech_monthly_device.rename(columns=dict(zip([attr["attribute_name"] for attr in tech_monthly_attr], [attr["code"] for attr in tech_monthly_attr])))
#%%
tech_monthly_project = tech_monthly_device.groupby([
    "date",
    "project",
    # "logical_device",
    # "iec_eqpt_code",
]).agg({
    "act_energy_consumption": "sum",
    "act_energy_generation": "sum",
    "act_energy_iec": "sum",
    "act_energy": "sum",
    "act_prod_iec_revenue": "sum",
    "duration_full_perf": "sum",
    "duration_no_full_perf": "sum",
    "duration_running": "sum",
    "gained_energy_iec_consolidated": "sum",
    "gained_energy_iec_PC": "sum",
    "gained_prod_iec_revenue": "sum",
    "line_count": "sum",
    "lost_energy_iec_consolidated": "sum",
    "lost_energy_iec_PC": "sum",
    "lost_prod_iec_revenue": "sum",
    "olf_iec_management_denom": "sum",
    "olf_iec_management_num": "sum",
    "olf_wo_environmental_denom": "sum",
    "olf_wo_environmental_num": "sum",
    "pba_edf_om_denom": "sum",
    "pba_edf_om_num": "sum",
    "pba_iec_op_denom": "sum",
    "pba_iec_op_num": "sum",
    "pba_management_denom": "sum",
    "pba_management_num": "sum",
    "pba_owner_denom": "sum",
    "pba_owner_num": "sum",
    "pot_energy_iec_consolidated": "sum",
    "pot_energy_iec_consolidated_when_avail": "sum",
    "pot_energy_PC": "sum",
    "pot_energy_PC_when_avail": "sum",
    "pot_prod_iec_revenue": "sum",
    "rr_act_power_iec_pot_power_iec_consolidated": "mean",
    "rr_act_power_iec": "mean",
    "rr_pot_power_iec_consolidated": "mean",
    "rr_wind_speed_act_power_iec": "mean",
    "rr_wind_speed": "mean",
    "tba_iec_om_denom": "sum",
    "tba_iec_om_num": "sum",
    "tba_iec_op_denom": "sum",
    "tba_iec_op_num": "sum",
    "wind_speed_avg": "mean",
})

#%%
cont_monthly_attr = [
# "month_asset_act_energy_iec", # Already in technical view
# "month_asset_act_prod_iec_revenue",
# "month_asset_gained_energy_iec_consolidated",
# "month_asset_gained_prod_iec_revenue",
# "month_asset_lost_energy_iec_consolidated",
# "month_asset_lost_prod_iec_revenue",
# "month_asset_pot_energy_iec_consolidated",
# "month_asset_pot_prod_iec_revenue",
# "month_asset_rr_wind_speed_consolidated",
# "month_asset_wind_speed_consolidated",
"monthly_asset_act_energy_iec_not_excluded",
"monthly_asset_contract_availability_denom",
"monthly_asset_contract_availability_num",
"monthly_asset_contract_pba_denom",
"monthly_asset_contract_pba_num",
"monthly_asset_contract_tba_denom",
"monthly_asset_contract_tba_num",
"monthly_asset_duration",
"monthly_asset_gained_energy_iec_consolidated_not_excluded",
"monthly_asset_lost_energy_iec_consolidated_contractor",
"monthly_asset_lost_energy_iec_consolidated_excluded",
"monthly_asset_lost_energy_iec_consolidated_not_excluded",
"monthly_asset_lost_energy_iec_consolidated_owner",
"monthly_asset_pot_energy_iec_consolidated_not_excluded",
"monthly_contract_availability_info",
# "rr_act_power_iec",
# "rr_act_power_iec_pot_power_iec_consolidated",
# "rr_pot_power_iec_consolidated",
# "rr_wind_speed_consolidated_act_power_iec",
]

# cont_monthly_attr = rdl_api.technical_views(
#                 "engine_attributes",
#             filters=[
#             {
#             "column": "endpoint",
#             "value": "cont_monthly_contract_agg"
#             },
#             {
#             "column": "mode",
#             "value": "reclassified"
#             }
#             ] 
#             )

cont_monthly_device = []
for project in tqdm.tqdm(project_list):
    try:
        cont_monthly_device.append(
            rdl_api.contractual_monthly_asset(                 
                start_date,
                end_date,
                project["project_code"],
                # attributes=[attr["code"] for attr in cont_monthly_attr],
                attributes=cont_monthly_attr,
                output_type="json",
                as_df=False
            )

            )
    except:
        pass

cont_monthly_device = pd.concat([pd.DataFrame(p) for p in cont_monthly_device])


cont_monthly_device = cont_monthly_device.replace("nan", np.nan)
cont_monthly_device = cont_monthly_device.apply(lambda x: pd.to_numeric(x, errors="ignore"))
cont_monthly_device["date"] = pd.to_datetime(cont_monthly_device["date"], format="%Y-%m", dayfirst=True)
cont_monthly_device["project"] = cont_monthly_device["logical_device"].str[:4]

#%%
cont_monthly_project = cont_monthly_device.groupby([
    "date",
    "project"
]).agg({
    "monthly_asset_act_energy_iec_not_excluded": "sum",
    "monthly_asset_contract_availability_denom": "sum",
    "monthly_asset_contract_availability_num": "sum",
    "monthly_asset_contract_pba_denom": "sum",
    "monthly_asset_contract_pba_num": "sum",
    "monthly_asset_contract_tba_denom": "sum",
    "monthly_asset_contract_tba_num": "sum",
    "monthly_asset_duration": "sum", # A vérifier
    "monthly_asset_gained_energy_iec_consolidated_not_excluded": "sum",
    "monthly_asset_lost_energy_iec_consolidated_contractor": "sum",
    "monthly_asset_lost_energy_iec_consolidated_excluded": "sum",
    "monthly_asset_lost_energy_iec_consolidated_not_excluded": "sum",
    "monthly_asset_lost_energy_iec_consolidated_owner": "sum",
    "monthly_asset_pot_energy_iec_consolidated_not_excluded": "sum",
    "monthly_contract_availability_info": "sum",
})

all_merged_df = tech_monthly_project.copy().reset_index()
all_merged_df = all_merged_df.merge(
    cont_monthly_project.reset_index(),
    left_on=["project", "date"],
    right_on=["project", "date"],
    how="outer"
)

for availability_type in [
    "pba_edf_om", 
    "pba_iec_op", 
    "pba_management", 
    "pba_owner", 
    "tba_iec_om", 
    "tba_iec_op",
    "monthly_asset_contract_availability",
    "monthly_asset_contract_pba",
    "monthly_asset_contract_tba",
    ]:
    all_merged_df[availability_type] = all_merged_df[f"{availability_type}_num"] / all_merged_df[f"{availability_type}_denom"]

rdl_to_bluepoint = {
    "date": "Date (start)",
    "project": "Plant ID",
    "monthly_asset_contract_availability": "OEM Availability",
    "pba_owner": "Availability",
    "pot_energy_iec_consolidated": "Wind Theoretical Performance",
    "pot_energy_iec_consolidated_when_avail": "Weather Adjusted Production",
    "wind_speed_avg": "Wind Speed",
    # "act_power_avg": "Power",
    "act_energy": "Production",
}

all_merged_df = all_merged_df.rename(columns=rdl_to_bluepoint)[list(rdl_to_bluepoint.values())]
all_merged_df["Interval"] = "Monthly"
all_merged_df = pd.melt(
    all_merged_df,
    id_vars=["Plant ID", "Date (start)", "Interval"],
    var_name="Meter",
    value_name="Value",
).dropna(subset=["Value"])

all_merged_df.loc[
    all_merged_df["Meter"].isin(["OEM Availability", "Availability"]), 
    "Value"] = all_merged_df.loc[
    all_merged_df["Meter"].isin(["OEM Availability", "Availability"]), 
    "Value"]*100

all_merged_df["Value"] = (
    all_merged_df["Value"].astype(float).round(5)
)

all_merged_df.to_excel(os.path.join(
    "/home/EDF/centralparc/06.Import regulier/01.RDL/01.Brut",
    f"scada_{start_date.replace('-', '')}_{end_date.replace('-', '')}.xlsx"
),
index=False
)