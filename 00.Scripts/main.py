#%%
import os
import re
import glob

import pandas as pd

data_folder_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\01.Snapshot"
export_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\03.Bluepoint import data"
objet_type_to_bluepoint_config_file = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\03 - Paramètrage\parametrage_objets.xlsx"

all_bluepoint_id_df = pd.read_excel("correspondance_identifiants_plateforme.xlsx", header=[1])

windga_to_bluepoint_id = []
for id, gr in all_bluepoint_id_df.groupby(["ID_CENTRALE", "CODE_PI"]):
    windga_to_bluepoint_id.append([id[0], id[1], ";".join(gr["Plant ID"].tolist()), gr["Location ID"].iloc[0]])
windga_to_bluepoint_id = pd.DataFrame(windga_to_bluepoint_id, columns=["ID_CENTRALE", "CODE_PI", "Plant ID", "Location ID"])

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


#%%
contrat_type_to_bluepoint = (
        pd.read_excel(
                objet_type_to_bluepoint_config_file,
                sheet_name="2 - Contrats windga_bluepoint"
                )
        .dropna(subset=["Nom Bluepoint - décision équipe"])[["ID_TYPE_ENTITE_ELEMENTS", "Nom Bluepoint - décision équipe"]]
        .set_index("ID_TYPE_ENTITE_ELEMENTS")
        .to_dict()["Nom Bluepoint - décision équipe"]
        )

#%%
def dms_to_decimal(DMS: str) -> float:
    try:
        if DMS is None:
            return None

        else:
            split = re.split(r'[°\'\’"]+', DMS)
            degrees = float(split[0]) if split[0] else None
            minutes = float(split[1]) if split[1] else None
            seconds = float(split[2].replace(",", ".")) if split[2] else None
            direction = split[3] if len(split) == 4 else None
            if degrees is not None and minutes is not None and seconds is not None and direction in ["N", "S", "E", "W", "O"]:
                result = (degrees + minutes / 60 + seconds / 3600) * (1 if direction in ["N", "E"] else -1)
                return result
            else:
                return None
    except:
        return None
    
centrale_df = pd.read_parquet(os.path.join(data_folder_path, "centrale.parquet"))
projet_df = pd.read_parquet(os.path.join(data_folder_path, "projet.parquet"))
type_entite_element_df = pd.read_parquet(os.path.join(data_folder_path, "type_entite_element.parquet"))
type_projet_df = pd.read_parquet(os.path.join(data_folder_path, "type_projet.parquet"))
equipement_df = pd.read_parquet(os.path.join(data_folder_path, "equipement.parquet"))
materiel_df = pd.read_parquet(os.path.join(data_folder_path, "materiel.parquet"))
centrale_localisation_df = pd.read_parquet(os.path.join(data_folder_path, "centrale_localisation.parquet"))
localisation_df = pd.read_parquet(os.path.join(data_folder_path, "localisation.parquet"))
interlocuteur_df = pd.read_parquet(os.path.join(data_folder_path, "interlocuteur.parquet"))
interlocuteur_df["RAISON_SOCIALE"] = interlocuteur_df["RAISON_SOCIALE"].str.title()
interlocuteur_df["NOM_INTERLOCUTEUR"] = interlocuteur_df["RAISON_SOCIALE"].str.title()
centrale_interlocuteur_df = pd.read_parquet(os.path.join(data_folder_path, "centrale_interlocuteur.parquet"))
projet_interlocuteur_df = pd.read_excel(os.path.join(data_folder_path, "projet_interlocuteur.xlsx"))
contrat_df = pd.read_parquet(os.path.join(data_folder_path, "contrat.parquet"))
contrat_interlocuteur_df = pd.read_excel(os.path.join(data_folder_path, "contrat_interlocuteur.xlsx")).dropna(subset=["ID_INTERLOCUTEUR"])
contrat_interlocuteur_df = contrat_interlocuteur_df.sort_values("DATE_MODIFICATION").groupby("ID_CONTRAT").last().reset_index()
contrat_df = contrat_df.merge(contrat_interlocuteur_df[["ID_CONTRAT", "ID_INTERLOCUTEUR"]], left_on=["ID_CONTRAT"], right_on=["ID_CONTRAT"], how="left")
equipement_df = pd.read_parquet(os.path.join(data_folder_path, "equipement.parquet"))
materiel_df = pd.read_parquet(os.path.join(data_folder_path, "materiel.parquet"))
evenement_df = pd.read_parquet(os.path.join(data_folder_path, "evenements.parquet"))
liste_evenement_df = pd.read_parquet(os.path.join(data_folder_path, "liste_evenements.parquet"))

#%%

projet_df = projet_df.merge(
        type_projet_df[["ID_TYPE_PROJET", "LIBELLE_TYPE_PROJET"]], 
        left_on=["ID_TYPE_PROJET"], 
        right_on=["ID_TYPE_PROJET"], 
        how="left")

equipement_df = equipement_df.merge(materiel_df[["ID_MATERIEL", "ID_TYPE_MATERIEL"]], left_on=["ID_MATERIEL"], right_on=["ID_MATERIEL"], how="left").merge(
        type_entite_element_df[["ID_TYPE_ENTITE_ELEMENTS", "LIBELLE_ENTITE"]], 
        left_on=["ID_TYPE_MATERIEL"], 
        right_on=["ID_TYPE_ENTITE_ELEMENTS"], 
        how="left").rename(columns={"LIBELLE_ENTITE": "TYPE_MATERIEL"})

centrale_df = centrale_df.merge(
        projet_df[["ID_PROJET", "LIBELLE_TYPE_PROJET", "CODE_PROJET_SAP"]],
        how="left"
)

centrale_df["LATITUDE_DE_LA_CENTRALE"] = centrale_df["LATITUDE_DE_LA_CENTRALE"].apply(dms_to_decimal).round(6)
centrale_df["LONGITUDE_DE_LA_CENTRALE"] = centrale_df["LONGITUDE_DE_LA_CENTRALE"].apply(dms_to_decimal).round(6)

centrale_df = (centrale_df
.merge(
    equipement_df[equipement_df["TYPE_MATERIEL"]=="PDL"][["ID_CENTRALE", "PUISSANCE_RACCORDEMENT"]].groupby("ID_CENTRALE").sum().reset_index(),
    left_on=["ID_CENTRALE"],
    right_on=["ID_CENTRALE"],
    how="left"
     )
.merge(
    pd.read_excel(os.path.join(data_folder_path, "portfolios.xlsx")), 
    how="left"
    ))

centrale_df = centrale_df.drop(columns=["CODE_POSTAL"]).merge(
    centrale_localisation_df,
    how="left"
).merge(
    localisation_df,
    left_on=["ID_LOCALISATION"],
    right_on=["ID_LOCALISATION"],
    how="left"
)
asset_manager_df = interlocuteur_df.merge(
    centrale_interlocuteur_df,
    left_on=["ID_INTERLOCUTEUR"],
    right_on=["ID_INTERLOCUTEUR"],
    how="right"
)
asset_manager_df = (asset_manager_df.loc[asset_manager_df["ID_TYPE_RESPONSABILITE"]==91]
                    .sort_values("DATE_MODIFICATION_y")
                    .groupby("ID_CENTRALE")
                    .first()
                    .reset_index()
)

projet_asset_manager_df = interlocuteur_df.merge(
    projet_interlocuteur_df[projet_interlocuteur_df["FLG_ACTIF"]==True],
    left_on=["ID_INTERLOCUTEUR"],
    right_on=["ID_INTERLOCUTEUR"],
    how="right"
).merge(
    centrale_df[["ID_PROJET", "ID_CENTRALE"]],
    left_on=["ID_PROJET"],
    right_on=["ID_PROJET"],
    how="left"
).drop(columns=["ID_PROJET"])

projet_asset_manager_df = (projet_asset_manager_df[projet_asset_manager_df["ID_TYPE_RESPONSABILITE"]==91]
                           .dropna(subset="ID_CENTRALE")
                           .sort_values("DATE_MODIFICATION_y")
                           .groupby("ID_CENTRALE")
                           .first()
                           .reset_index()
)

asset_manager_df = pd.concat([
    asset_manager_df[["ID_CENTRALE", "EMAIL_INTERLOCUTEUR"]],
    projet_asset_manager_df[["ID_CENTRALE", "EMAIL_INTERLOCUTEUR"]]
]).rename(columns={"EMAIL_INTERLOCUTEUR": "Asset Manager"})

centrale_df = centrale_df.merge(
    asset_manager_df,
    how="left"
)

centrale_df = centrale_interlocuteur_df[centrale_interlocuteur_df["FLG_ACTIF"]][["ID_CENTRALE", "ID_INTERLOCUTEUR"]].merge(
        interlocuteur_df[interlocuteur_df["ID_TYPE_INTERLOCUTEUR"] == 106][["ID_INTERLOCUTEUR", "NOM_INTERLOCUTEUR", "RAISON_SOCIALE", "CODE_SAP", "ID_TYPE_INTERLOCUTEUR"]],
        left_on=["ID_INTERLOCUTEUR"],
        right_on=["ID_INTERLOCUTEUR"],
        how="right"
)[["ID_CENTRALE", "RAISON_SOCIALE"]].merge(
    centrale_df,
    left_on=["ID_CENTRALE"],
    right_on=["ID_CENTRALE"],
    how="right"
)

centrale_df = centrale_df.drop_duplicates(subset=["ID_CENTRALE"])
#%%
wtg_df = equipement_df[equipement_df["ID_TYPE_MATERIEL"]==161][["ID_CENTRALE", "ID_MATERIEL"]].merge(materiel_df)
centrale_df = centrale_df.merge(
    wtg_df.groupby("ID_CENTRALE").first().reset_index()[["ID_CENTRALE", "DIAMETRE_ROTOR", "HAUTEUR_MIN"]],
    how="left"
)

#%%
import_asset_manager_df = centrale_df[["Asset Manager"]].drop_duplicates()
import_asset_manager_df["First Name"] = import_asset_manager_df["Asset Manager"].str.split(".").str[0].str.title()
import_asset_manager_df["First Name"] = import_asset_manager_df["Asset Manager"].str.split(".").str[1].str.split("@").str[0].str.title()


#%%
# 	Description Notes	
import_plant_df = all_bluepoint_id_df[["Plant ID", "Location ID", "CODE_PROJET_SAP", "ID_CENTRALE"]]
import_plant_df = import_plant_df.merge(
    centrale_df[["ID_CENTRALE", "NOM_CENTRALE", "PUISSANCE_RACCORDEMENT", "LIBELLE_TYPE_PROJET", "Asset Manager", 
                 "ADRESSE_CENTRALE_LIGNE1", "ADRESSE_CENTRALE_LIGNE2", "CODE_POSTAL", "LATITUDE_DE_LA_CENTRALE", 
                 "LONGITUDE_DE_LA_CENTRALE", "VILLE", "Portfolios", "RAISON_SOCIALE", "DIAMETRE_ROTOR", "HAUTEUR_MIN", "Portfolios"]],
    left_on=["ID_CENTRALE"],
    right_on=["ID_CENTRALE"],
    how="left"
)

import_plant_df = import_plant_df.rename(columns={
    "NOM_CENTRALE": "Plant Name",
    "PUISSANCE_RACCORDEMENT": "Capacity AC",
    "CODE_PROJET_SAP": "Code eOTP SAP",
    "LIBELLE_TYPE_PROJET": "Resource",
    "LATITUDE_DE_LA_CENTRALE": "Latitude",
    "LONGITUDE_DE_LA_CENTRALE": "Longitude",
    "ADRESSE_CENTRALE_LIGNE1": "Address Line 1",
    "ADRESSE_CENTRALE_LIGNE2": "Address Line 2",
    "CODE_POSTAL": "ZIP Code",
    "VILLE": "City",
    "RAISON_SOCIALE": "Project Company",
    "DIAMETRE_ROTOR": "Rotor Length",
    "HAUTEUR_MIN": "Turbine Height",
})

import_plant_df["Location Name"] = import_plant_df["Plant Name"]
import_plant_df["County"] = ""
import_plant_df["State"] = ""
import_plant_df["Status"] = "operational"
import_plant_df["Country"] = "France"
import_plant_df["Utility Company"] = "Enedis"
import_plant_df["Type"] = "Ground mount"
import_plant_df["Contacts"] = ""
import_plant_df["Overview Notes"] = ""
import_plant_df["Voltages"] = "20.78kV"
import_plant_df["Description Notes	"] = ""
import_plant_df["Altitude"] = 0
import_plant_df["Resource"] = import_plant_df["Resource"].replace({
        "Eolien": "Wind",
        "PV Sol ": "Solar",
        "Ombrière et parking": "Solar"
})

import_plant_df["Shading"] = "none"
import_plant_df["Biome"] = ""
import_plant_df["Protected Wildlife"] = ""
import_plant_df["Risk notes"] = ""
import_plant_df["Area Notes"] = ""
import_plant_df["Area Size"] = ""
import_plant_df["Enclosure Type"] = ""
import_plant_df["Position"] = ""
import_plant_df["Primary Key Holder"] = ""
import_plant_df["Secondary Key Holder"] = ""
import_plant_df["Access Notes"] = ""
import_plant_df["Tags"] = "A vérifier"
import_plant_df[import_plant_df["Resource"] == "Wind"][import_plant_df["Location ID"].isin(limit_scope_to)].to_excel(os.path.join(export_path, "3.plant_wind.xlsx"), index=False)

import_plant_df = import_plant_df.drop(columns=["Rotor Length", "Turbine Height"])
import_plant_df["Tilt Angle"] = 0
import_plant_df["Azimuth Angle"] = 0
import_plant_df["Multiple Tilt and Azimuth"] = "No"
import_plant_df["Soil"] = "rock layer"
import_plant_df[import_plant_df["Resource"] == "Solar"].rename(columns={"Capacity AC": "Capacity DC"}
).drop_duplicates()[import_plant_df["Location ID"].isin(limit_scope_to)].to_excel(
    os.path.join(export_path, "4.plant_solar.xlsx"), 
    index=False
    )

#%%
companies_df = pd.read_excel(r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\interlocuteur_nettoye.xlsx")

#%%

import_contract_df = (contrat_df[[
    "ID_TYPE_CONTRAT", 
    "NOM_CONTRAT",
    "ID_CENTRALE",
    "DATE_DEBUT",
    "DATE_FIN",
    "COMMENTAIRES",
    "ID_INTERLOCUTEUR",
    ]].merge(
        centrale_df[["ID_CENTRALE", "RAISON_SOCIALE"]],
        how="left"
    )
    .rename(columns={"RAISON_SOCIALE": "Client (Company)",})
    .merge(
        companies_df[["ID_INTERLOCUTEUR", "Company Name"]],
        how="left"
    )
    .merge(
        windga_to_bluepoint_id,
        how="left"
    )
.rename(columns={
        "NOM_CONTRAT": "Contract Name",
        "Company Name": "Vendor (Company)",
        "DATE_DEBUT": "Start Date",
        "DATE_FIN": "End Date",
        "COMMENTAIRES": "Service Description",
        "Plant ID": "Plant Identifier",
        "ID_TYPE_CONTRAT": "Contract Type"
    })
.dropna(subset=["Plant Identifier"])
.merge(
    all_bluepoint_id_df[["Location ID", "ID_CENTRALE"]].drop_duplicates(subset=["ID_CENTRALE"]),
    how="left"
)
.drop(columns=["ID_INTERLOCUTEUR", "ID_CENTRALE", "ID_INTERLOCUTEUR"])
)

import_contract_df = import_contract_df[import_contract_df["Contract Type"] != 137]
import_contract_df["Contract Type"] = import_contract_df["Contract Type"].replace(contrat_type_to_bluepoint)
import_contract_df = import_contract_df[import_contract_df["Contract Type"].isin(contrat_type_to_bluepoint.values())]

for col in import_contract_df.columns:
    try:
        import_contract_df[col] = import_contract_df[col].str.replace("\t", " ")
        import_contract_df[col] = import_contract_df[col].str.replace("\t\n", " ")
        import_contract_df[col] = import_contract_df[col].str.replace("\n", " ")
        import_contract_df[col] = import_contract_df[col].str.replace("\r", " ")
    except:
        pass

import_contract_df["Contract Name"] = (import_contract_df["Plant Identifier"].str[:4]  \
                                + "-" \
                                + import_contract_df["Contract Type"].astype(str) \
                                + "-" \
                                + import_contract_df["Start Date"].dt.year.astype(str)).str[:-2]

import_contract_df["Tags"] = "A vérifier"
import_contract_df["Contract Date"] = import_contract_df["Start Date"]
import_contract_df.loc[import_contract_df["Contract Type"].str.contains("Vente"), 
                       ["Client (Company)", "Vendor (Company)"]] = import_contract_df.loc[import_contract_df["Contract Type"].str.contains("Vente"), 
                       ["Vendor (Company)", "Client (Company)"]]

import_contract_df[import_contract_df["Location ID"].isin(limit_scope_to)].to_csv(
    os.path.join(export_path, "10.contract.csv"),
    index=False,
    date_format="%d/%m/%Y",
    sep=";"
    )
import_contract_df

pd.concat([
    import_contract_df[import_contract_df["Location ID"].isin(limit_scope_to)]["Client (Company)"],
    import_contract_df[import_contract_df["Location ID"].isin(limit_scope_to)]["Vendor (Company)"]
]).drop_duplicates().to_csv(
    os.path.join(export_path, "2.company.csv"),
    index=False,
    date_format="%d/%m/%Y",
    sep=";"
    )
#%%
event_type_to_bluepoint = (
        pd.read_excel(
                objet_type_to_bluepoint_config_file,
                sheet_name="4 - Event wsp_bluepoint"
                )
        .dropna(subset=["Nom Bluepoint - décision équipe"])[["Nom WSP", "Nom Bluepoint - décision équipe"]]
        .set_index("Nom WSP")
        .to_dict()["Nom Bluepoint - décision équipe"]
        )

event_type_to_bluepoint = (
        pd.read_excel(
                objet_type_to_bluepoint_config_file,
                sheet_name="4 - Event wsp_bluepoint"
                )
        .dropna(subset=["Nom Bluepoint - décision équipe"])[["Nom WSP", "Nom Bluepoint - décision équipe"]]
        .set_index("Nom WSP")
        .to_dict()["Nom Bluepoint - décision équipe"]
        )

event_to_bluepoint = {
# "ID": "Event ID",
"Parc": "Parc",
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
import_event_df["Asset Type"] = "Location"
import_event_df["Status"] = import_event_df["Status"].replace({
"En cours": "in_progress",
"Clos": "closed",
})
import_event_df["Event Type"] = import_event_df["Event Type"].replace(event_type_to_bluepoint)

import_event_df = import_event_df.merge(
    windga_to_bluepoint_id[["CODE_PI", "Location ID"]].drop_duplicates(subset=["CODE_PI"]),
    left_on=["Parc"],
    right_on=["CODE_PI"],
    how="left"
).rename(columns={"Location ID": "Location Identifier"}
         ).drop(columns=["CODE_PI", "Parc"])

if limit_scope_to:
    import_event_df = import_event_df[import_event_df["Location Identifier"].isin(limit_scope_to)]

import_event_df["Event Name"] = import_event_df["Location Identifier"] + " - " + import_event_df["Root Cause"].astype(str)
import_event_df["Est. Revenue Loss"] = import_event_df["Est. Revenue Loss"].astype(float)
import_event_df.to_csv(
    os.path.join(export_path, "event_wsp.csv"), 
    sep=";", 
    index=False, 
    date_format="%d/%m/%Y")

#%%

meters_df = pd.concat(
    [
        pd.read_csv(file_path, parse_dates=[0], decimal=",")
        for file_path in glob.glob(os.path.join(data_folder_path, "meters", "*.csv"))
    ]
)

meters_df = meters_df[meters_df["project"].str.len() == 4]

meters_to_bluepoint = {
    "project": "project",
    "date": "Date (start)",
    "Interval": "Interval",
    "monthly_asset_contract_tba": "Availability",
    "act_energy_consumption": "Consumption",
    "act_energy_iec": "Gross Generation",
    # "monthly_asset_contract_availability": "OEM Availability",
    # "duration_running": "Operational Hours",
    "act_energy": "Production",
    "wind_speed_avg": "Wind Speed",
    "pot_energy_iec_consolidated": "Wind Theoretical Production",
}

import_meter_df = meters_df.copy()

import_meter_df["Interval"] = "Monthly"

import_meter_df = import_meter_df[list(meters_to_bluepoint.keys())].rename(
    columns=meters_to_bluepoint
)

import_meter_df[["Availability"]] = 100*import_meter_df[["Availability"]]

import_meter_df = pd.melt(
    import_meter_df,
    id_vars=["project", "Date (start)", "Interval"],
    var_name="Meter",
    value_name="Value",
).dropna(subset=["Value"])

import_meter_df["Value"] = (
    import_meter_df["Value"].astype(float).round(5)
)

import_meter_df = import_meter_df.merge(
    all_bluepoint_id_df[["RDL project", "Plant ID"]].drop_duplicates(subset=["RDL project"]),
    left_on=["project"],
    right_on=["RDL project"],
    how="left"
)

if limit_scope_to:
    import_meter_df = import_meter_df[import_meter_df["project"].str[:4].isin([
        "ANPA",
        "BVER",
        "JON2",
        "JONC",
        "ROUS",
        "FIEN",
        "CHPI",
        "MA24",
        "BRIA",
        "COLB",
        "NITR",
        "JAVI",
        "LEMO",
        "SALL",
        "VARA",
    ])]


import_meter_df.drop(columns=["RDL project", "project"]).to_csv(
    os.path.join(export_path, "meters.csv"),
    sep=";",
    index=False,
    date_format="%d/%m/%Y",
)

#%%
productible_theorique_df = pd.read_parquet(os.path.join(data_folder_path, "productible_theorique.parquet"))
type_productible_theorique_df = pd.read_parquet(os.path.join(data_folder_path, "type_productible_theorique.parquet"))
productible_theorique_mensuel_df = pd.read_parquet(os.path.join(data_folder_path, "productible_theorique_mensuel.parquet"))

prod_df = productible_theorique_df.merge(
    type_productible_theorique_df[type_productible_theorique_df["LIBELLE_TYPE_PRODUCTIBLE_THEO"]=="CEPG"][[
        "ID_TYPE_PRODUCTIBLE_THEO", 
        "LIBELLE_TYPE_PRODUCTIBLE_THEO"]],
    left_on=["ID_TYPE_PRODUCTIBLE"],
    right_on=["ID_TYPE_PRODUCTIBLE_THEO"],
    how="right"
)

windga_forcast_to_bluepoint = {
    "PRODUCTION": "Forecast Generation",
    "GISEMENT_THEO": "Forecast Irradiation",
    "PR": "Forecast PR",
    "DISPO_AUTRE1": "Foract Availability",
}

# PROD_REF	P50_NET	P75_NET	P90_NET	PERTES_ELEC_INTERNE	PERTES_AUTRE1	
# PERTES_AUTRE2	PERTES_AUTRE3	DISPO_GENERATEUR	DISPO_RESEAU	
# DISPO_AUTRE1	DISPO_AUTRE2	DISPO_AUTRE3	VENT_MOYEN1
# VENT_MOYEN2	GISEMENT_THEO	PR_AN1	PR_MOYEN

prod_df = prod_df.merge(
    all_bluepoint_id_df[["Plant ID", "ID_CENTRALE"]].drop_duplicates(subset=["ID_CENTRALE"]),
    how="left"
).drop_duplicates(subset=["Plant ID"])[[
    "ID_CENTRALE",
    "PROD_REF",
    "P50_NET",
    "P75_NET",
    "P90_NET",	
]]

prod_df["PROD_ANNUELLE"] = prod_df[["PROD_REF",
    "P50_NET",
    "P75_NET",
    "P90_NET",	]].max(axis=1)

productible_theorique_df = productible_theorique_df.merge(
    type_productible_theorique_df[type_productible_theorique_df["CATEGORIE_TYPE_PRODUCTIBLE_THEO"]=="BUDGET"][[
        "ID_TYPE_PRODUCTIBLE_THEO", 
        "LIBELLE_TYPE_PRODUCTIBLE_THEO"]],
    left_on=["ID_TYPE_PRODUCTIBLE"],
    right_on=["ID_TYPE_PRODUCTIBLE_THEO"],
    how="left"
)

productible_theorique_mensuel_df = productible_theorique_mensuel_df.merge(
    productible_theorique_df[["ID_PRODUCTIBLE_THEO", "LIBELLE_TYPE_PRODUCTIBLE_THEO", "ID_CENTRALE"]],
)

productible_theorique_mensuel_df = productible_theorique_mensuel_df.merge(
    all_bluepoint_id_df[["Plant ID", "ID_CENTRALE"]].drop_duplicates(subset=["ID_CENTRALE"]),
    how="left"
)

# Forecast Availability
# Forecast Generation
# Forecast Irradiation
# Forecast PR



import_forcast_df = productible_theorique_mensuel_df.copy()
import_forcast_df = import_forcast_df.rename(columns=windga_forcast_to_bluepoint)

import_forcast_df["annee"] = import_forcast_df["LIBELLE_TYPE_PRODUCTIBLE_THEO"].str.extract(r"(\d{4})")
import_forcast_df = import_forcast_df.dropna(subset=["annee"]).dropna(subset=["MOIS"])
import_forcast_df["Date (start)"] = import_forcast_df.apply(lambda x: pd.Timestamp(year=int(x["annee"]), month=int(x["MOIS"]), day=1 ), axis=1)
import_forcast_df["Interval"] = "Monthly"

import_forcast_df = import_forcast_df.merge(
    prod_df[["ID_CENTRALE", "PROD_ANNUELLE"]],
    left_on=["ID_CENTRALE"],
    right_on=["ID_CENTRALE"],
    how="left"
)
import_forcast_df["Forecast Generation"] = import_forcast_df["PROD_ANNUELLE"] * import_forcast_df["Forecast Generation"]

#%%
import_forcast_df = pd.melt(
    import_forcast_df,
    id_vars=["Plant ID", "Date (start)", "Interval"],
    var_name="Meter",
    value_name="Value",
).dropna(subset=["Value"]).dropna(subset=["Plant ID"])

import_forcast_df = import_forcast_df[import_forcast_df["Meter"].isin(list(windga_forcast_to_bluepoint.values()))]

import_forcast_df["Value"] = (
    import_forcast_df["Value"].astype(float).round(5)
)

if limit_scope_to:
    import_forcast_df = import_forcast_df[import_forcast_df["Plant ID"].str[:4].isin([
        "ANPA",
        "BVER",
        "JON2",
        "JONC",
        "ROUS",
        "FIEN",
        "CHPI",
        "BRIA",
        "COLB",
        "NITR",
        "JAVI",
        "LEMO",
        "SALL",
        "VARA",
    ])]


import_forcast_df.to_csv(
    os.path.join(export_path, "forcast_meters.csv"),
    sep=";",
    index=False,
    date_format="%d/%m/%Y",
)

#%%
compteur_df = pd.concat([pd.read_excel(
    r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\01.Snapshot\eveler_enedis.xlsx",
    sheet_name="Enedis"
),
pd.read_excel(
    r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\01.Snapshot\eveler_enedis.xlsx",
    sheet_name="Eveler"
)])

compteur_df = compteur_df.merge(
    all_bluepoint_id_df[["Plant ID", "Compteur eveler", "Location ID"]],
    left_on=["meter"],
    right_on=["Compteur eveler"]
)

compteur_df["source"]  =compteur_df["source"].replace({
    "Enedis": "Utility Statement",
    "Eveler": "Export",
})



compteur_df = compteur_df[compteur_df["Plant ID"].str[:4].isin([
    "ANPA",
    "BVER",
    "JON2",
    "JONC",
    "ROUS",
    "FIEN",
    "CHPI",
    "BRIA",
    "COLB",
    "NITR",
    "JAVI",
    "LEMO",
    "SALL",
    "VARA",
    "PEZI",
    "ROUS"
])]

compteur_df = compteur_df[compteur_df["date"].dt.year==2023]
compteur_df["Interval"] = "Monthly"

compteur_df = compteur_df.rename(columns={
    "date": "Date (start)",
    "production": "Value",
    "source": "Meter",
})

compteur_df[["Plant ID", "Meter", "Date (start)", "Interval", "Value"]].to_csv(
    os.path.join(export_path, "compteur.csv"),
    sep=";",
    index=False,
    date_format="%d/%m/%Y",
)

#%%
equipement_df = equipement_df[["ID_EQUIPEMENT_MATERIEL", "ID_EQUIPEMENT_MATERIEL_PARENT", "ID_CENTRALE", 
                               "ID_MATERIEL", "IDENTIFIANT", "QUANTITE"]].merge(
    materiel_df[["ID_MATERIEL", "LIBELLE_MATERIEL", "MODELE", "ID_TYPE_MATERIEL"]],
    how="left"
).merge(
    type_entite_element_df[["ID_TYPE_ENTITE_ELEMENTS", "CODE_TYPE_ENTITE_ELEMENT", "LIBELLE_ENTITE"]],
    left_on=["ID_TYPE_MATERIEL"],
    right_on=["ID_TYPE_ENTITE_ELEMENTS"],
    how="left"
).merge(
    centrale_df[["ID_CENTRALE", "Asset Manager"]],
    how="left"
)

#%%
# Select equipement wher the asset maanager is lola.gautier@€df-re.fr and where the type of equipement is CODE_TYPE_ENTITE_ELEMENT

equipement_df = equipement_df[
    (equipement_df["Asset Manager"]=="lola.gautier@edf-re.fr") &
    (equipement_df["CODE_TYPE_ENTITE_ELEMENT"].isin(["MODULE", "ONDULEUR_STRING", "ONDULEUR"]))
    ]# %%

#%%
milestone_type_to_bluepoint = (
        pd.read_excel(
                objet_type_to_bluepoint_config_file,
                sheet_name="6 - Jalons windga_bluepoint"
                )
        .dropna(subset=["Nom Bluepoint - décision équipe"])[["ID_EVENEMENT", "Nom Bluepoint - décision équipe"]]
        .set_index("ID_EVENEMENT")
        .to_dict()["Nom Bluepoint - décision équipe"]
        )

import_milestone_df = liste_evenement_df[["ID_EVENEMENT", "ID_CENTRALE", "ID_PROJET", "ID_STATUT", "DATE_FIN"]].merge(
    evenement_df[["ID_EVENEMENT", "ID_TYPE_EVENEMENT", "LIBELLE_EVENEMENT"]],
    left_on="ID_EVENEMENT",
    right_on="ID_EVENEMENT",
    suffixes=["", "milestone"],
    how="left"
).merge(
    all_bluepoint_id_df[["ID_CENTRALE", "Plant ID"]],
    how="left"
).rename(columns={"DATE_FIN": "Date"})

import_milestone_df = import_milestone_df[import_milestone_df["ID_EVENEMENT"].isin(list(milestone_type_to_bluepoint.keys()))]
import_milestone_df["Milestone Type"] = import_milestone_df["ID_EVENEMENT"].replace(milestone_type_to_bluepoint)

import_milestone_df = import_milestone_df[~import_milestone_df["Date"].isna()]

import_milestone_df["Actual"] = "Yes"
# import_milestone_df["ID_STATUT"] = import_milestone_df["ID_STATUT"].replace(milestone_to_bluepoint)

import_milestone_df
# import_milestone_df.to_excel(os.path.join(export_path, "5.milestone.xlsx"), index=False)

#%% List of asset
def read_list_of_asset_sheet(sheet_name, file_path="List of assets_FR_20200701.xlsx"):
    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name
            )
        # test = df.copy()
        df = df.iloc[15:, 3:10]
        df = df.dropna(subset=["Unnamed: 3"])
        df = df.dropna(subset=["Unnamed: 7"])
        df = df[["Unnamed: 3", "Unnamed: 7", "Unnamed: 9"]]
        df.columns=  ["Variable", "Value", "Note"]
        df["Code PI"] = sheet_name

        return df
    except:
        print(sheet_name)
        return pd.DataFrame()

#%%
list_of_asset_df = pd.concat([
    read_list_of_asset_sheet(
        sheet_name, 
        file_path=r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\02 - Ateliers\Manager GA\Copie de List of assets_FR_20200701.xlsx"
                             )
    for sheet_name in pd.ExcelFile(
        r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\02 - Ateliers\Manager GA\Copie de List of assets_FR_20200701.xlsx").sheet_names 
        if len(sheet_name) == 4
])