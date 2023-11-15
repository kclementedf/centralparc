#%%
import os
import re

import pandas as pd

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

data_folder_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\01.Snapshot"
export_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\03.Bluepoint import data"
objet_type_to_bluepoint_config_file = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\03 - Paramètrage\parametrage_objets.xlsx"

event_type_to_bluepoint = (
        pd.read_excel(
                objet_type_to_bluepoint_config_file,
                sheet_name="4 - Event wsp_bluepoint"
                )
        .dropna(subset=["Nom Bluepoint - décision équipe"])[["Nom WSP", "Nom Bluepoint - décision équipe"]]
        .set_index("Nom WSP")
        .to_dict()["Nom Bluepoint - décision équipe"]
        )

#%% WSP
event_to_bluepoint = {
# "ID": "Event ID",
"Title": "Plant Identifiers",
"Catégorie": "Event Type",
"Avancement": "Status",
# "Nb machines": "machines",
"Commentaire": "Resolution Notes",
"Début": "Event Date",
"Fin": "Date Resolved",
"Pertes": "Est. Revenue Loss",
"Origine": "Root Cause",
# "Actions correctives": "correctives",
# "Recouvrement": "",
# "Infos diverses": "diverses",
# "Composant impacté": "impacté",
# "Item Type": "Type",
# "Path": "",	
}

wsp_df = pd.read_excel(os.path.join(
    data_folder_path,
    "wsp.xlsx",
))

import_event_df = wsp_df.copy()

import_event_df = import_event_df[list(event_to_bluepoint.keys())].rename(columns=event_to_bluepoint)
import_event_df["Asset Type"] = "Plants"
import_event_df["Status"] = import_event_df["Status"].replace({
"En cours": "in_progress",
"Clos": "closed",
})
import_event_df["Event Type"] = import_event_df["Event Type"].replace(event_type_to_bluepoint)
import_event_df["Event Name"] = import_event_df["Plant Identifiers"] + " - " + import_event_df["Root Cause"].astype(str)

if limit_scope_to:
    import_event_df = import_event_df[import_event_df["Plant Identifiers"].isin(limit_scope_to)]

import_event_df.iloc[135:].to_csv(os.path.join(export_path, "event_wsp.csv"), sep=";", index=False, date_format="%d/%m/%Y")
