#%%
import os
import re
from sys import platform

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


if platform == "windows":
    data_folder_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 03 - Réalisation - 03 - Réalisation\02.Migration données\01.Snapshot"
    export_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 03 - Réalisation - 03 - Réalisation\02.Migration données\03.Import data\01.Brut"
    objet_type_to_bluepoint_config_file = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 03 - Réalisation - 03 - Réalisation\01.Bluepoint\parametrage_objets.xlsx"

if platform == "linux":
    data_folder_path = r"/home/EDF/centralparc/01.Snapshot"
    export_path = r"//home/EDF/centralparc/03.Import data/01.Brut"
    objet_type_to_bluepoint_config_file = r"/home/EDF/centralparc/parametrage_objets.xlsx"

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

import_event_df["Resolution Notes"] = import_event_df["Resolution Notes"].str.replace(r"\r\n", " ")
import_event_df["Resolution Notes"] = import_event_df["Resolution Notes"].str.replace("\r\n", " ")
import_event_df["Resolution Notes"] = import_event_df["Resolution Notes"].str.replace("\n", " ")


if limit_scope_to:
    import_event_df = import_event_df[import_event_df["Plant Identifiers"].isin(limit_scope_to)]

import_event_df.to_excel(
    os.path.join(export_path, "11.event_wsp.xlsx"),
    index=False,
    #sep=",",
    # date_format="%d/%m/%Y"
    )
