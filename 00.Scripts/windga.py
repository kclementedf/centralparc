# %%
import os
import re
from sys import platform

import pandas as pd
from rapidfuzz import fuzz

limit_scope_to = None

# limit_scope_to = [
# "ANPA",
# "BVER",
# "JON2",
# "JONC",
# "ROUS",
# "CY11",
# "FIEN",
# "MA22",
# "TL11",
# "PEZI",
# "CHPI",
# "TL12",
# "TL13",
# "TL14",
# "TL15",
# "MA24",
# "CY12",
# "CY13",
# "BRIA",
# "COLB",
# "NITR",
# "JAVI",
# "LEMO",
# "SALL",
# "COT1",
# "COT2",
# "COT3",
# "COT4",
# "VARA",
# ]

# Set this varaibles differently for windows and linux

if platform == "win32":
    data_folder_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\01.Snapshot"
    export_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\centralparc\03.Import data\01.Brut"
    objet_type_to_bluepoint_config_file = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\03 - Paramètrage\parametrage_objets.xlsx"

if platform == "linux":
    data_folder_path = r"/home/EDF/centralparc/01.Snapshot"
    export_path = r"//home/EDF/centralparc/03.Import data/01.Brut"
    objet_type_to_bluepoint_config_file = r"/home/EDF/centralparc/parametrage_objets.xlsx"

correpondance_id_df = pd.read_excel(os.path.join(
    "..",
    r"correspondance_identifiants_plateforme.xlsx"
),
    header=1,
)

contrat_type_to_bluepoint = (
    pd.read_excel(
        objet_type_to_bluepoint_config_file,
        sheet_name="2 - Contrats windga_bluepoint"
    )
    .dropna(subset=["Nom Bluepoint - décision équipe"])[["ID_TYPE_ENTITE_ELEMENTS", "Nom Bluepoint - décision équipe"]]
    .set_index("ID_TYPE_ENTITE_ELEMENTS")
    .to_dict()["Nom Bluepoint - décision équipe"]
)

# materiel -> component
materiel_type_to_bluepoint = (
    pd.read_excel(
        objet_type_to_bluepoint_config_file,
        sheet_name="8 - Materiel windga_bluepoint"
    )
    .dropna(subset=["Nom Bluepoint - décision équipe"])[["ID_TYPE_ENTITE_ELEMENTS", "Nom Bluepoint - décision équipe"]]
    .set_index("ID_TYPE_ENTITE_ELEMENTS")
    .to_dict()["Nom Bluepoint - décision équipe"]
)

milestone_type_to_bluepoint = (
    pd.read_excel(
        objet_type_to_bluepoint_config_file,
        sheet_name="6 - Jalons windga_bluepoint"
    )
    .dropna(subset=["Nom Bluepoint - décision équipe"])[["ID_EVENEMENT", "Nom Bluepoint - décision équipe"]]
    .set_index("ID_EVENEMENT")
    .to_dict()["Nom Bluepoint - décision équipe"]
)

antenne_to_bluepoint = (
    pd.read_excel(
        objet_type_to_bluepoint_config_file,
        sheet_name="14 - Portefeuille windga_bluepo"
    )
    .dropna(subset=["ID_ANTENNE"])[["ID_ANTENNE", "Bluepoint portefeuille"]]
    .set_index("ID_ANTENNE")
    .to_dict()["Bluepoint portefeuille"]
)

pole_to_bluepoint = {
    1: "Eolien Nord",
    2: "Eolien Sud",
    3: "PV & DOM",
}

# %%


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
                result = (degrees + minutes / 60 + seconds / 3600) * \
                    (1 if direction in ["N", "E"] else -1)
                return result
            else:
                return None
    except:
        return None


centrale_df = pd.read_parquet(os.path.join(
    data_folder_path, "centrale.parquet"))

if limit_scope_to:
    centrale_df = centrale_df[centrale_df["CODE_PI"].isin(limit_scope_to)]

projet_df = pd.read_parquet(os.path.join(data_folder_path, "projet.parquet"))

type_entite_element_df = pd.read_parquet(os.path.join(
    data_folder_path, "type_entite_element.parquet"))

type_projet_df = pd.read_parquet(os.path.join(
    data_folder_path, "type_projet.parquet"))

equipement_df = pd.read_parquet(os.path.join(
    data_folder_path, "equipement.parquet"))

materiel_df = pd.read_parquet(os.path.join(
    data_folder_path, "materiel.parquet"))

contrat_df = pd.read_parquet(os.path.join(data_folder_path, "contrat.parquet"))

centrale_statut_df = pd.read_parquet(os.path.join(
    data_folder_path, "centrale_statut.parquet"))

statut_df = pd.read_parquet(os.path.join(data_folder_path, "statut.parquet"))

evenement_df = pd.read_parquet(os.path.join(
    data_folder_path, "evenements.parquet"))

liste_evenement_df = pd.read_parquet(os.path.join(
    data_folder_path, "liste_evenements.parquet"))

# productible_reel_mensuel_df = pd.read_parquet(os.path.join(data_folder_path, "productible_reel_mensuel.parquet"))
productible_reel_mensuel_df = pd.read_excel(os.path.join(
    data_folder_path,
    r"productible_reel_mensuel.xlsx"
))
productible_reel_mensuel_df["PERIODE_MENSUELLE"] = pd.to_datetime(
    productible_reel_mensuel_df["PERIODE_MENSUELLE"])

# productible_reel_mensuel_detail_df = pd.read_parquet(os.path.join(data_folder_path, "productible_reel_mensuel_detail.parquet"))
productible_reel_mensuel_detail_df = pd.read_excel(os.path.join(
    data_folder_path, 
    r"productible_reel_mensuel_detail.xlsx"
    ))
productible_reel_mensuel_detail_df["PERIODE_MENSUELLE"] = pd.to_datetime(productible_reel_mensuel_detail_df["PERIODE_MENSUELLE"])


interlocuteur_df = pd.read_parquet(os.path.join(
    data_folder_path, "interlocuteur.parquet"))
interlocuteur_df["RAISON_SOCIALE"] = interlocuteur_df["RAISON_SOCIALE"].str.title()
interlocuteur_df["NOM_INTERLOCUTEUR"] = interlocuteur_df["RAISON_SOCIALE"].str.title()

centrale_interlocuteur_df = pd.read_parquet(os.path.join(
    data_folder_path, "centrale_interlocuteur.parquet"))

projet_interlocuteur_df = pd.read_excel(os.path.join(
    data_folder_path, "projet_interlocuteur.xlsx"))

localisation_df = pd.read_parquet(os.path.join(
    data_folder_path, "localisation.parquet"))

centrale_localisation_df = pd.read_parquet(os.path.join(
    data_folder_path, "centrale_localisation.parquet"))

# meter_list = pd.read_excel(os.path.join(data_folder_path, "adm_meter_wt_association.xlsx"))

antenne = pd.read_excel(os.path.join(data_folder_path, "antenne.xlsx"))

productible_theorique_df = pd.read_parquet(os.path.join(
    data_folder_path, "productible_theorique.parquet"))

type_productible_theorique_df = pd.read_parquet(os.path.join(
    data_folder_path, "type_productible_theorique.parquet"))

productible_theorique_mensuel_df = pd.read_parquet(os.path.join(
    data_folder_path, "productible_theorique_mensuel.parquet"))

contrat_interlocuteur = pd.read_excel(os.path.join(
    data_folder_path, "contrat_interlocuteur.xlsx"))

sap_company_df = pd.read_excel(os.path.join(
    data_folder_path,
    "SAP",
    "livraison 202310",
    "Extraction Central Parc_API Companies BP.xlsx"
),
    sheet_name="API Companies BP")

centrale_evenement_df = pd.read_excel(os.path.join(
    data_folder_path, "centrale_evenement.xlsx"))
projet_evenement_df = pd.read_excel(os.path.join(
    data_folder_path, "projet_evenement.xlsx"))

# %%
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
# %%
centrale_df = centrale_df.merge(
    projet_df[["ID_PROJET", "LIBELLE_TYPE_PROJET", "CODE_PROJET_SAP"]],
    how="left"
)

centrale_df["LATITUDE_DE_LA_CENTRALE"] = centrale_df["LATITUDE_DE_LA_CENTRALE"].apply(
    dms_to_decimal).round(6)
centrale_df["LONGITUDE_DE_LA_CENTRALE"] = centrale_df["LONGITUDE_DE_LA_CENTRALE"].apply(
    dms_to_decimal).round(6)

# Ajout de la puissance de la centrale
centrale_df = (centrale_df
               .merge(
                   equipement_df[equipement_df["TYPE_MATERIEL"] == "PDL"][[
                       "ID_CENTRALE", "PUISSANCE_RACCORDEMENT"]].groupby("ID_CENTRALE").sum().reset_index(),
                   left_on=["ID_CENTRALE"],
                   right_on=["ID_CENTRALE"],
                   how="left"
               )
               .merge(
                   pd.read_excel(os.path.join(
                       data_folder_path, "portfolios.xlsx")),
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

centrale_df = centrale_df.merge(
    correpondance_id_df[["ID_CENTRALE", "Location ID", "Plant ID"]],
    left_on=["ID_CENTRALE"],
    right_on=["ID_CENTRALE"],
    how="left"
).dropna(subset=["Plant ID"])

# %%
asset_manager_df = interlocuteur_df.merge(
    centrale_interlocuteur_df,
    left_on=["ID_INTERLOCUTEUR"],
    right_on=["ID_INTERLOCUTEUR"],
    how="right"
)
asset_manager_df = (asset_manager_df.loc[asset_manager_df["ID_TYPE_RESPONSABILITE"] == 91]
                    .sort_values("DATE_MODIFICATION_y")
                    .groupby("ID_CENTRALE")
                    .first()
                    .reset_index()
                    )

projet_asset_manager_df = interlocuteur_df.merge(
    projet_interlocuteur_df[projet_interlocuteur_df["FLG_ACTIF"] == True],
    left_on=["ID_INTERLOCUTEUR"],
    right_on=["ID_INTERLOCUTEUR"],
    how="right"
).merge(
    centrale_df[["ID_PROJET", "ID_CENTRALE"]],
    left_on=["ID_PROJET"],
    right_on=["ID_PROJET"],
    how="left"
).drop(columns=["ID_PROJET"])

projet_asset_manager_df = (projet_asset_manager_df[projet_asset_manager_df["ID_TYPE_RESPONSABILITE"] == 91]
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

asset_manager_df = asset_manager_df.groupby("ID_CENTRALE").last().reset_index()

centrale_df = centrale_df.merge(
    asset_manager_df,
    how="left"
)

centrale_df = centrale_interlocuteur_df[centrale_interlocuteur_df["FLG_ACTIF"]][["ID_CENTRALE", "ID_INTERLOCUTEUR"]].merge(
    # 106 -> Société projet
    interlocuteur_df[interlocuteur_df["ID_TYPE_INTERLOCUTEUR"] == 106][[
        "ID_INTERLOCUTEUR", "NOM_INTERLOCUTEUR", "RAISON_SOCIALE", "CODE_SAP", "ID_TYPE_INTERLOCUTEUR"]],
    left_on=["ID_INTERLOCUTEUR"],
    right_on=["ID_INTERLOCUTEUR"],
    how="right"
)[["ID_CENTRALE", "RAISON_SOCIALE"]].merge(
    centrale_df,
    left_on=["ID_CENTRALE"],
    right_on=["ID_CENTRALE"],
    how="right"
)

sap_project_company_df = pd.read_excel(
    os.path.join(
        data_folder_path,
        "SAP",
        "livraison 202310",
        "Extractions Central Parc_API Companies code.xlsx"
    ),
    sheet_name="API Companies code"
)
# sap_project_company_df

# Add a column to centrale_df with the closest match from sap_project_company_df["Company name"] to centrale_df["RAISON_SOCIALE"]
# centrale_df["Company Name"] = centrale_df["RAISON_SOCIALE"].apply(lambda x: process.extractOne(x, sap_project_company_df["Company name"])[0])
# centrale_df["Company Number"] = centrale_df["Company Name"].apply(lambda x: sap_project_company_df[sap_project_company_df["Company name"] == x]["Company code"].values[0])

cross = centrale_df[['Plant ID', "RAISON_SOCIALE"]].merge(
    sap_project_company_df[['Company Name']], how='cross')
cross['match_acc'] = cross.apply(lambda x: fuzz.ratio(
    x["RAISON_SOCIALE"], x["Company Name"]), axis=1)
centrale_df = centrale_df.merge(
    cross.sort_values(["Plant ID", "match_acc"], ascending=[
                      True, False]).drop_duplicates(["Plant ID"])[["Plant ID", "Company Name"]],
    how='left'
)


# %%
centrale_df["antenne"] = centrale_df.apply(
    lambda x: x["ID_ANTENNE_PDL"] if x["ID_ANTENNE_PDL"] is not None else x["ID_ANTENNE"], axis=1)
centrale_df["antenne"] = centrale_df["antenne"].round(0)
centrale_df["antenne"] = centrale_df["antenne"].replace(
    antenne_to_bluepoint).fillna("")

centrale_df["ID_POLE"] = centrale_df["ID_POLE"].round(
    0).replace(pole_to_bluepoint).fillna("")

# %%
centrale_df["Portfolios"] = centrale_df.apply(lambda x: ";".join([
    x["antenne"],
    x["ID_POLE"]
]), axis=1)


# %% SAS projet -> Récupération depuis SAP

# sas_to_bluepoint = {
#         "RAISON_SOCIALE": "Company Name",
#         "Is Project Company?": "Is Project Company?",
#         "CODE_SAP": "Company Number",
# }
# import_project_companies_df = centrale_df[["Plant ID", "ID_CENTRALE"]].merge(
#     centrale_interlocuteur_df[centrale_interlocuteur_df["FLG_ACTIF"]][["ID_CENTRALE", "ID_INTERLOCUTEUR"]],
#     left_on=["ID_CENTRALE"],
#     right_on=["ID_CENTRALE"],
#     how="left"
# ).merge(
#         interlocuteur_df[["ID_INTERLOCUTEUR", "NOM_INTERLOCUTEUR", "RAISON_SOCIALE", "CODE_SAP", "ID_TYPE_INTERLOCUTEUR"]],
#         left_on=["ID_INTERLOCUTEUR"],
#         right_on=["ID_INTERLOCUTEUR"],
#         how="left"
# )

# import_project_companies_df = import_project_companies_df[import_project_companies_df["ID_TYPE_INTERLOCUTEUR"] == 106]
# import_project_companies_df["Is Project Company?"] = "Yes"

# import_project_companies_df = import_project_companies_df[list(sas_to_bluepoint.keys())].rename(columns=sas_to_bluepoint)
# import_project_companies_df = import_project_companies_df.drop_duplicates()
# import_project_companies_df["Company Number"] = import_project_companies_df["Company Number"].fillna("")


# %%
# import_project_companies_df.drop_duplicates().to_excel(
#     os.path.join(export_path, "1.project_companies.xlsx"),
#     index=False,
# date_format="%d/%m/%Y"
# )

# %%
import_plant_df = centrale_df.copy().drop(columns=["CODE_PI"])
import_plant_df["Status"] = "Operational"
import_plant_df["Country"] = "France"
import_plant_df["Location Name"] = import_plant_df["NOM_CENTRALE"].values
import_plant_df["Utility Company"] = "Enedis"
# # import_plant_df["Location ID"] = import_plant_df["CODE_PI"].values

# location_df = centrale_df.copy()
# location_df = location_df.groupby("ID_PROJET").agg({"ID_CENTRALE": "count", "CODE_PI": "first", "NOM_CENTRALE": "first"}).reset_index()
# location_df.loc[location_df["ID_CENTRALE"] > 1, "CODE_PI"] = location_df.loc[location_df["ID_CENTRALE"] > 1, "CODE_PI"].str[:3] + "X" # cas de centrale avec plusieurs comptage
# location_df["CODE_PI"] = location_df["CODE_PI"].str[:4] # Enlève la partie METER00X du code pi
# location_df = location_df.rename(columns={"NOM_CENTRALE": "Location Name", "CODE_PI": "Location ID"})
# import_plant_df=  import_plant_df.merge(
#     location_df[["ID_PROJET", "Location Name", "Location ID"]],
#     left_on=["ID_PROJET"],
#     right_on=["ID_PROJET"],
#     how="left"
# )

centrale_to_bluepoint = {
    "NOM_CENTRALE": "Plant Name",
    "CODE_PI": "Plant ID",
    "CODE_PROJET_SAP": "Code eOTP SAP",
    "LIBELLE_TYPE_PROJET": "Resource",
    "Status": "Status",
    "Location Name": "Location Name",
    "Location ID": "Location ID",
    "LATITUDE_DE_LA_CENTRALE": "Latitude",
    "LONGITUDE_DE_LA_CENTRALE": "Longitude",
    "ADRESSE_CENTRALE_LIGNE1": "Address Line 1",
    "ADRESSE_CENTRALE_LIGNE2": "Address Line 2",
    "CODE_POSTAL": "ZIP Code",
    "VILLE": "City",
    "Country": "Country",
    "PUISSANCE_RACCORDEMENT": "Capacity AC",
    "Portfolios": "Portfolios",
    "Company Name": "Project Company",
    "Asset Manager": "Asset Manager",
}

import_plant_df = import_plant_df.rename(columns=centrale_to_bluepoint)[
    list(centrale_to_bluepoint.values())]
import_plant_df["Resource"] = import_plant_df["Resource"].replace({
    "Eolien": "Wind",
    "PV Sol ": "Solar",
    "Ombrière et parking": "Solar"
})

import_plant_df = import_plant_df.drop_duplicates(subset=["Plant ID"])

import_plant_df["Capacity DC"] = import_plant_df["Capacity AC"]
# import_plant_df.loc[import_plant_df["Resource"] == "Solar", "Capacity AC"] = None
# import_plant_df.loc[import_plant_df["Resource"] == "Wind", "Capacity DC"] = None
# import_plant_df.to_excel(os.path.join(export_path, "plant.xlsx"), sep=",", index=False)x

# import_plant_df.to_excel(os.path.join(export_path, "plant.xlsx"), sep=",", index=False)
import_plant_df[import_plant_df["Resource"] == "Wind"].drop(columns=["Capacity DC"]
                                                            ).to_excel(os.path.join(export_path, "3.plant_wind.xlsx"), index=False)


import_plant_df[import_plant_df["Resource"] == "Solar"].drop(columns=["Capacity AC"]
                                                             ).drop_duplicates().to_excel(os.path.join(export_path, "4.plant_solar.xlsx"), index=False)

# %% Contrats
contrat_df = (contrat_df
              .merge(centrale_df[["ID_CENTRALE", "Plant ID", "Company Name"]], how="inner")
              )
contrat_plant = {}
for contrat, gr in contrat_df.groupby("ID_CONTRAT"):
    contrat_plant.update({contrat: ";".join(gr["Plant ID"].tolist())})

contrat_df = contrat_df.groupby("ID_CONTRAT").first().reset_index()

contrat_df["Plant Identifier"] = contrat_df["ID_CONTRAT"].replace(
    contrat_plant)

contrat_df = contrat_df.merge(
    #                                                       142 -> Prestataire
    contrat_interlocuteur[contrat_interlocuteur["ID_TYPE_CONTACT"] == 142][[
        "ID_CONTRAT", "ID_INTERLOCUTEUR"]],
    left_on=["ID_CONTRAT"],
    right_on=["ID_CONTRAT"],
    how="left"
).merge(
    interlocuteur_df[["ID_INTERLOCUTEUR", "RAISON_SOCIALE"]],
    left_on=["ID_INTERLOCUTEUR"],
    right_on=["ID_INTERLOCUTEUR"],
    how="left"
)
#%%
# cross = contrat_df[['ID_CONTRAT', "RAISON_SOCIALE"]].merge(sap_company_df[['Company Name']].rename(columns={"Company Name": "Vendor"}), how='cross')
# cross['match_acc'] = cross.apply(lambda x: fuzz.ratio(x["RAISON_SOCIALE"], x["Vendor"]), axis=1)
# # contrat_df = 
# contrat_df = contrat_df.merge(
#     cross.sort_values(["ID_CONTRAT", "match_acc"], ascending=[True, False]).drop_duplicates(["ID_CONTRAT"])[["ID_CONTRAT", "Vendor"]],
#     how='left'
#     )

# %%
contrat_to_bluepoint = {
    "ID_TYPE_CONTRAT": "Contract Type",
    "NOM_CONTRAT": "Contract Name",
    "Plant Identifier": "Plant Identifier",
    "DATE_DEBUT": "Start Date",
    "DATE_FIN": "End Date",
    # "": "Vendor (Compagny)",
    # "": "Client (Compagny)",
    "COMMENTAIRES": "Service Description",
}

import_contract_df = contrat_df.copy()
import_contract_df = import_contract_df[(import_contract_df["ID_TYPE_CONTRAT"] != 137) & (
    ~import_contract_df["ID_TYPE_CONTRAT"].isna())]
import_contract_df["ID_TYPE_CONTRAT"] = import_contract_df["ID_TYPE_CONTRAT"].replace(
    contrat_type_to_bluepoint)
import_contract_df = import_contract_df.rename(columns=contrat_to_bluepoint)[
    list(contrat_to_bluepoint.values())]
import_contract_df = pd.concat([
    import_contract_df[import_contract_df["Contract Type"].str.contains(
        "Vente") == False],

    import_contract_df[import_contract_df["Contract Type"].str.contains(
        "Vente") == True].rename(columns={"Vendor": "Client", "Company Name": "Vendor"})
])

import_contract_df["Contract Name"] = (import_contract_df["Plant Identifier"].str[:4]
                                       + "-"
                                       + import_contract_df["Contract Type"].astype(str)
                                       + "-"
                                       + import_contract_df["Start Date"].dt.year.astype(str)).str[:-2]

import_contract_df["Contract Date"] = import_contract_df["Start Date"]

import_contract_df = import_contract_df.dropna(subset=["Contract Type"])

# for chunk_number, chunk in enumerate(range(100, import_contract_df.shape[0], 100)):
#         import_contract_df.iloc[max(0, chunk-100):chunk, :].to_excel(
#              os.path.join(export_path, f"contract_{chunk_number}.xlsx"),
#              sep=",",
#              index=False,
#              #date_format="%d/%m/%Y"
#              )

import_contract_df = import_contract_df.drop_duplicates(
    subset=["Plant Identifier", "Contract Type", "Start Date", "End Date"]
)

import_contract_df["Service Description"] = import_contract_df["Service Description"].str.replace(
    r"\r\n", " ")
import_contract_df["Service Description"] = import_contract_df["Service Description"].str.replace(
    "\r\n", " ")

import_contract_df.to_excel(
    os.path.join(export_path, f"10.contract.xlsx"),
    index=False,
    # date_format="%d/%m/%Y"
)

# %% Jalons
# milestone_to_bluepoint = {
#     "CODE_PI": "Plant ID",
#     "ID_STATUT": "Milestone Type",
#     "DATE_STATUT": "Date",
#     "Actual": "Actual",
# }

# import_milestone_df = (centrale_df
#                 .merge(
#                         centrale_statut_df,
#                         left_on="ID_CENTRALE",
#                         right_on="ID_CENTRALE",
#                         how='left')
#                 .merge(
#                         statut_df,
#                         left_on="ID_STATUT",
#                         right_on="ID_STATUT",
#                         how="left"
#                         )
#                 )

# %%
milestone_to_bluepoint = {
    "CODE_PI": "Plant ID",
    "ID_EVENEMENT": "Milestone Type",
    "DATE_DEBUT": "Date",
    "Actual": "Actual",
}
import_milestone_df = centrale_df.merge(
    liste_evenement_df,
    left_on="ID_CENTRALE",
    right_on="ID_CENTRALE",
    suffixes=["", "milestone"],
    how="left"
).merge(
    evenement_df,
    left_on="ID_EVENEMENT",
    right_on="ID_EVENEMENT",
    suffixes=["", "milestone"],
    how="left"
)

import_milestone_df = import_milestone_df[~import_milestone_df["DATE_DEBUT"].isna(
)]

import_milestone_df["Actual"] = "Yes"
import_milestone_df["ID_EVENEMENT"] = import_milestone_df["ID_EVENEMENT"].replace(
    milestone_to_bluepoint)

import_milestone_df = import_milestone_df[list(
    milestone_to_bluepoint.keys())].rename(columns=milestone_to_bluepoint)

import_milestone_df.to_excel(os.path.join(
    export_path, "5.milestone.xlsx"), index=False)

# %%
milestone_type_to_bluepoint = {
    "J3": "OR3",
    "MISE_SERVICE_TECHNIQUE": "",
    "FPR":	 "",
    "PTF_AR_ACCEPTATION": "",
    "DEMANDE_AUTORISATION": "",
    "AUTORISATION_PC":	 "",
    "STATUT_AUTORISATION_PC": "",
    "DEMANDE_AUTORISATION_UNIQUE": "",
    "STATUT_DEMANDE_AUTORISATION_UNIQUE": "",
    "AUTORISATION_DAU": "",
    "STATUT_AUTORISATION_DAU": "",
    "DEMARRAGE_PROJET": "",
    "AUTORISATION_ICPE": "Arrêté d'autorisation ICPE",
    "ACCORD_LANCEMENT_REALISATION": "",
    "OBJECTIF_PASSAGE_DEVELOPPEMENT_A": "",
    "PASSAGE_DEVELOPPEMENT_A": "",
    "OBJECTIF_DEPOT_DEMANDE_AUTORISATION":	"",
    "DEPOT_PC": "",
    "J0": "",
    "DATE_DEMANDE_CONTRAT_DACHAT": "",
    "PV_RECEPTION_LOTS": "GA1",
    "DEMARRAGE_CONSTRUCTION_CENTRALE": "",
    "PRISE_CHARGE_EXPLOITATION": "OR3",
    "DAACT": "",
    "PTF_AR_ACCEPTATION": "",
    "TRANSFERT_EENS": "",
    "ACTIVATION_PPA": "",
    "TRANSFERT_EEN_FR": "",
    "RECEPT_RECIPISSE_CONTRAT_ACHAT_EDF_AOA": "",
    "ENVOI_DEMANDE_CONTRAT_ACHAT_EDF_AOA": "",
    "SIGNATURE_CONTRAT_ACHAT_EDF_AOA": "",
    "AVENANT_PDB_ENVOYE": "",
    "AVENANT_PDB_SIGNE": "",
    "MST_CENTRALE_RESEAU": "",
}

milestone_df = pd.concat([
    centrale_df[["Plant ID", "ID_CENTRALE"]].merge(
        centrale_evenement_df,
        left_on="ID_CENTRALE",
        right_on="ID_CENTRALE",
        how="left"
    ),
    centrale_df[["Plant ID", "ID_CENTRALE", "ID_PROJET"]].merge(
        projet_evenement_df,
        left_on="ID_PROJET",
        right_on="ID_PROJET",
        how="left"
    )
])

milestone_df = milestone_df.drop(columns=["ID_PROJET", "ID_CENTRALE"]).melt(
    id_vars=["Plant ID"],
    value_name="Date",
    var_name="Milestone Type"
)
milestone_df["Date"] = pd.to_datetime(
    milestone_df["Date"], format='mixed', errors='coerce')
milestone_df["Actual"] = "Yes"
milestone_df["Milestone Type"] = milestone_df["Milestone Type"].replace(
    milestone_type_to_bluepoint)
milestone_df = milestone_df.dropna(subset=["Milestone Type"])
milestone_df = milestone_df[milestone_df["Milestone Type"] != ""]
milestone_df = milestone_df.dropna(subset=["Date"])
milestone_df.to_excel(os.path.join(
    export_path, "5.milestone.xlsx"), index=False)
# %% WSP
event_to_bluepoint = {
    "ID": "Event ID",
    "Title": "Plant Identifiers",
    "Catégorie": "Event Type",
    "Avancement": "Status",
    "Nb": "machines",
    "Commentaire": "Resolution Notes",
    "Début": "Event Date",
    "Fin": "Date Resolved",
    "Pertes": "Est. Revenue Loss",
    "Origine": "Root Cause",
    "Actions": "correctives",
    "Recouvrement": "",
    "Infos": "diverses",
    "Composant": "impacté",
    "Item": "Type",
    "Path": "",
}

# %% Meters
meters_to_bluepoint = {
    "Interval": "Interval",
    "Plant ID": "Plant ID",
    "LIBELLE_TYPE_PROJET": "LIBELLE_TYPE_PROJET",
    # "Meter": "Meter",
    # "ID_PRODUCTIBLE_REEL_MENSUEL": "",
    "PERIODE_MENSUELLE": "Date (start)",
    "PRODUCTION": "Export",
    "GISEMENT": "Resource",
    # "REVENU": "Total Revenue",
    "TEMPERATURE": "Ambient Temp",
    # "ID_CENTRALE": "",
    # "ID_TYPE_PROD_CONTRACTUEL": "",
    # "ID_PRODUCTIBLE_REEL_MENSUEL_DETAIL": "",
    # "ID_EQUIPEMENT_MATERIEL": "",
    # "DISPO_I1": "",
    # "DISPO_I2": "",
    # "DISPO_I3": "",
    # "DISPO_I4": "",
    # "DISPO_I5": "",
    # "DISPO_I6": "",
    # "DISPO_I7": "",
    "DISPO_SAISIE": "OEM Availability",
    # "PTHD": "",
    "PTH": "Weather Adjusted Production",
    "DISPO_ENERGIE_SAISIE": "Availability",
    # "ERPR": "",
    # "IRPR": "",
    "PR_SAISI": "Performance Ratio",
    # "DATE_VALID_DISPO_CONTRACTUELLE": "",
    "DISPO_TECHNIQUE_SAISI": "Availability",
}

import_meter_df = centrale_df[["ID_CENTRALE", "LIBELLE_TYPE_PROJET", "Plant ID"]].merge(
    productible_reel_mensuel_df,
    left_on=["ID_CENTRALE"],
    right_on=["ID_CENTRALE"],
    how="left"
).merge(
    productible_reel_mensuel_detail_df[productible_reel_mensuel_detail_df["ID_EQUIPEMENT_MATERIEL"].isna(
    )],
    left_on=["ID_PRODUCTIBLE_REEL_MENSUEL"],
    right_on=["ID_PRODUCTIBLE_REEL_MENSUEL"],
    how="left"
)

import_meter_df["Interval"] = "Monthly"

import_meter_df = import_meter_df[list(meters_to_bluepoint.keys())].rename(columns=meters_to_bluepoint)
import_meter_df["Availability"] = import_meter_df["Availability"] * 100
import_meter_df["OEM Availability"] = import_meter_df["OEM Availability"] * 100

import_meter_df = pd.melt(
    import_meter_df,
    id_vars=["Plant ID", "Date (start)", "Interval", "LIBELLE_TYPE_PROJET"],
    var_name="Meter",
    value_name="Value",
).dropna(subset=["Value"])

import_meter_df["Value"] = import_meter_df["Value"].astype(float).round(5)

import_meter_df.loc[(import_meter_df["LIBELLE_TYPE_PROJET"] == "Eolien") & (
    import_meter_df["Meter"] == "Resource"), "Meter"] = "Wind speed"
import_meter_df.loc[(import_meter_df["LIBELLE_TYPE_PROJET"] == "PV Sol") & (
    import_meter_df["Meter"] == "Resource"), "Meter"] = "Irradiation"

# import_meter_df[import_meter_df["Plant ID"]=="ROUS"].drop(columns=["LIBELLE_TYPE_PROJET"]).to_excel(
import_meter_df[import_meter_df["Date (start)"].dt.year == 2023].drop(columns=["LIBELLE_TYPE_PROJET"]).to_excel(
    os.path.join(export_path, "6.windga_meters.xlsx"),
    index=False,
    # date_format="%d/%m/%Y"
)

# %% Grilles tarifaires
# "\\FRDEFCP-FS01\Business\OMEGA\Private\BUDGET FACTURATION\0-FACTURATION\00-INDICES\Archives\Calcul indexation par site - 20-21.xls"

# %%
# single_meter_plant =
type_productible_theorique_df = pd.read_parquet(os.path.join(
    data_folder_path, "type_productible_theorique.parquet"))

productible_theorique_df = pd.read_parquet(os.path.join(
    data_folder_path, "productible_theorique.parquet"))
productible_theorique_df = productible_theorique_df.merge(
    centrale_df[["ID_CENTRALE", "Plant ID"]],
    how="left"
)

productible_theorique_mensuel_df = pd.read_parquet(os.path.join(
    data_folder_path, "productible_theorique_mensuel.parquet"))
productible_theorique_mensuel_df["MOIS"] = productible_theorique_mensuel_df["MOIS"].replace({
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December"
})
productible_theorique_mensuel_df = pd.pivot_table(
    productible_theorique_mensuel_df,
    index=["ID_PRODUCTIBLE_THEO"],
    columns=["MOIS"],
    values=["PRODUCTION", "GISEMENT", "PR", "REVENU", "TEMPERATURE_AMBIANTE_MOYENNE", "RAYONNEMENT_INCLINE"]
)

# Récupération ventilation production
productible_etude_df = productible_theorique_df.loc[
    (productible_theorique_df["ID_TYPE_PRODUCTIBLE"]==1)
    & (productible_theorique_df["FLG_ACTIF"])
    & (~productible_theorique_df["Plant ID"].isna()),
    :
]

productible_etude_df = pd.melt(
    productible_theorique_df[["Plant ID", "ID_PRODUCTIBLE_THEO", "P50_NET", "P75_NET", "P90_NET"]],
    id_vars=["Plant ID", "ID_PRODUCTIBLE_THEO"],
    var_name="Exceedance Probability",
    # value_name=
).dropna(subset=["value"]).merge(
    productible_theorique_mensuel_df["PRODUCTION"].reset_index(),
    how="left"
)
productible_etude_df

# %% Export

# %% Equipements
equipement_df = pd.read_parquet(os.path.join(
    data_folder_path, "equipement.parquet"))
equipement_df = equipement_df.merge(materiel_df[["ID_MATERIEL", "ID_TYPE_MATERIEL"]], left_on=["ID_MATERIEL"], right_on=["ID_MATERIEL"], how="left").merge(
    type_entite_element_df[["ID_TYPE_ENTITE_ELEMENTS", "LIBELLE_ENTITE"]],
    left_on=["ID_TYPE_MATERIEL"],
    right_on=["ID_TYPE_ENTITE_ELEMENTS"],
    how="left").rename(columns={"LIBELLE_ENTITE": "TYPE_MATERIEL"})
equipement_df = equipement_df.merge(
    correpondance_id_df[["ID_CENTRALE", "Plant ID"]],
    how="left"
).dropna(subset=["Plant ID"])

equipement_df["IDENTIFIANT"] = equipement_df["Plant ID"] + "-" + equipement_df["IDENTIFIANT"] 

equipement_df = equipement_df.merge(
    materiel_df[["ID_MATERIEL", "MODELE"]].rename(columns={"MODELE": "Model"}),
    how="left"
)

equipement_df = equipement_df.merge(
    equipement_df[["ID_EQUIPEMENT_MATERIEL", "IDENTIFIANT"]].rename(columns={"IDENTIFIANT": "Parent Component"}),
    left_on=["ID_EQUIPEMENT_MATERIEL_PARENT"],
    right_on=["ID_EQUIPEMENT_MATERIEL"],
    how="left"
)
#%%
import_wtg_df = equipement_df.copy()
import_wtg_df = import_wtg_df.loc[
    import_wtg_df["TYPE_MATERIEL"]=="WTG",
    [
    "Plant ID",
    "TYPE_MATERIEL",
    "Parent Component",
    # "OBSERVATION",
    "QUANTITE",
    "IDENTIFIANT",
    # 'TRANSFO_MARQUE',
    # 'TRANSFO_MODELE',
    'DATE_MISE_EN_SERVICE',
    'ALIAS_WTG',
    'TURBINIER_WTG',
    'TURBINE_MODELE',
    'TURBINE_MARQUE',
    'TURBINE_NUM_SERIE',
    # 'FABRICANT_CAPTEUR',
    # 'REFERENCE_CAPTEUR',
    # 'TYPE_SIGNAL_CAPTEUR',
    # 'TYPE_CAMERA_VIDEO_SURVEILLANCE',
    # 'NB_CAMERA_VIDEO_SURVEILLANCE',
    # 'FABRICANT_MODULE_PV',
    # 'FABRICANT_ONDULEUR',
    # 'REFERENCE_ONDULEUR',
    # 'REFERENCE_TRACKER',
]]

# import_wtg_df = import_wtg_df.rename(columns={
#     "TYPE_MATERIEL": "Component Type",
#     ""
# })

import_module_df = equipement_df.copy()
import_module_df = import_module_df.loc[import_module_df["TYPE_MATERIEL"]=="MODULE_PV", :]


import_module_df = import_module_df.rename(columns={
    "TYPE_MATERIEL": "Component Type",
    "IDENTIFIANT": "Component ID",
    "QUANTITE": "Quantity",
    "DATE_MISE_EN_SERVICE": "Date Installed",
    "FABRICANT_MODULE_PV": "Manufacturer",
})

import_module_df = import_module_df[[
    "Plant ID",
    "Model",
    "Component Type",
    "Component ID",
    "Parent Component",
    "Quantity",
    "Date Installed",
    "Manufacturer",
    "Model", # ??
]]

import_onduleur_df = equipement_df.copy()
import_onduleur_df = import_onduleur_df.loc[
     import_onduleur_df["TYPE_MATERIEL"]=="ONDULEUR",
     [
    "Plant ID",
    "Model",
    "TYPE_MATERIEL",
    "Parent Component",
    # "OBSERVATION",
    "QUANTITE",
    "IDENTIFIANT",
    # 'TRANSFO_MARQUE',
    # 'TRANSFO_MODELE',
    'DATE_MISE_EN_SERVICE',
    # 'ALIAS_WTG',
    # 'TURBINIER_WTG',
    # 'TURBINE_MODELE',
    # 'TURBINE_MARQUE',
    # 'TURBINE_NUM_SERIE',
    # 'FABRICANT_CAPTEUR',
    # 'REFERENCE_CAPTEUR',
    # 'TYPE_SIGNAL_CAPTEUR',
    # 'TYPE_CAMERA_VIDEO_SURVEILLANCE',
    # 'NB_CAMERA_VIDEO_SURVEILLANCE',
#     'FABRICANT_MODULE_PV',
#     'TYPE_MODULE',
#  'CADRES_MODULE_PV',
#  'ENTREPRISE_MODULE_PV',
#   'NB_MODULE_PV',
    'FABRICANT_ONDULEUR',
    'REFERENCE_ONDULEUR',
    # 'REFERENCE_TRACKER',
]
]

import_onduleur_df = import_onduleur_df.rename(columns={
"TYPE_MATERIEL": "Component Type",
"QUANTITE": "Quantity",
"IDENTIFIANT": "Component ID",
"DATE_MISE_EN_SERVICE": "Date Installed",
"FABRICANT_ONDULEUR": "Manufacturer",
"REFERENCE_ONDULEUR": "Serial Number" # ??
})

import_pdl_df = equipement_df.copy()
import_pdl_df = import_pdl_df.loc[
     import_pdl_df["TYPE_MATERIEL"]=="PDL",
     [
    "Plant ID",
    "Model",
    "TYPE_MATERIEL",
    "Parent Component",
    # "OBSERVATION",
    "QUANTITE",
    "IDENTIFIANT",
    # 'TRANSFO_MARQUE',
    # 'TRANSFO_MODELE',
    'DATE_MISE_EN_SERVICE',
    # 'ALIAS_WTG',
    # 'TURBINIER_WTG',
    # 'TURBINE_MODELE',
    # 'TURBINE_MARQUE',
    # 'TURBINE_NUM_SERIE',
    # 'FABRICANT_CAPTEUR',
    # 'REFERENCE_CAPTEUR',
    # 'TYPE_SIGNAL_CAPTEUR',
    # 'TYPE_CAMERA_VIDEO_SURVEILLANCE',
    # 'NB_CAMERA_VIDEO_SURVEILLANCE',
#     'FABRICANT_MODULE_PV',
#     'TYPE_MODULE',
#  'CADRES_MODULE_PV',
#  'ENTREPRISE_MODULE_PV',
#   'NB_MODULE_PV',
    # 'FABRICANT_ONDULEUR',
    # 'REFERENCE_ONDULEUR',
    # 'REFERENCE_TRACKER',
]
]

import_pdl_df = import_pdl_df.rename(columns={
"TYPE_MATERIEL": "Component Type",
"QUANTITE": "Quantity",
"IDENTIFIANT": "Component ID",
"DATE_MISE_EN_SERVICE": "Date Installed",
})
import_pdl_df

import_pdl_df.to_excel(
    os.path.join(export_path, "component_pdl.xlsx"),
    index=False,
    # date_format="%d/%m/%Y"
    )
import_onduleur_df.to_excel(
    os.path.join(export_path, "component_onduleur.xlsx"),
    index=False,
    # date_format="%d/%m/%Y"
    )
import_module_df.to_excel(
    os.path.join(export_path, "component_module.xlsx"),
    index=False,
    # date_format="%d/%m/%Y"
    )