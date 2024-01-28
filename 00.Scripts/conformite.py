import requests
import pandas as pd
from sys import platform

if platform == "windows":
    data_folder_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 03 - Réalisation - 03 - Réalisation\02.Migration données\01.Snapshot"
    export_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 03 - Réalisation - 03 - Réalisation\02.Migration données\03.Bluepoint import data"
    objet_type_to_bluepoint_config_file = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 03 - Réalisation - 03 - Réalisation\01.Bluepoint\parametrage_objets.xlsx"

if platform == "linux":
    data_folder_path = r"/home/EDF/centralparc/01.Snapshot"
    export_path = r"//home/EDF/centralparc/03.Import data/01.Brut"
    objet_type_to_bluepoint_config_file = r"/home/EDF/centralparc/parametrage_objets.xlsx"


conformite_list_df = pd.read_excel(
    objet_type_to_bluepoint_config_file,
    sheet_name="11 - Type conformité bluepoint",
    )

def get_plant_list(apiKey: str = None, field_to_keep: list = []):
    plant_df = pd.DataFrame(requests.get(
        "https://api.bluepoint.io/v1/plants/",
        headers={
            "Authorization": "JWT eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjbGllbnQiOjQzODIsInJhbmRvbV9rZXkiOiJqSGhWQjlKeDVXZEMifQ.MmEvKvrK_tw-ehTEG0y4XNxT4LJ0EO9hQThHOGWxmro",
            "Accept": "application/json"
        },
        verify=False
    ).json()["data"])[["identifier", "name", "resource", "location", "custom_fields", *field_to_keep]]
    plant_df = plant_df.rename(columns={"identifier": "Plant ID", "name": "Plant Name"})
    plant_df["Resource"] = plant_df["resource"].apply(lambda x: dict(x)["name"])
    plant_df["Code SAP"] = plant_df["custom_fields"].apply(lambda x: dict(x)["code-sap-0"])
    
    plant_df = pd.concat([
        plant_df,
        pd.DataFrame(plant_df["location"].tolist()).rename(columns={"identifier": "Location ID", "name": "Location Name", "latitude": "Latitude", "longitude": "Longitude"})], 
        axis=1
        )
    
    plant_df = plant_df.drop(columns=["custom_fields", "resource", "location"])
    
    return plant_df

plant_df = get_plant_list()
conformite_list_df = conformite_list_df.iloc[:11,:]

conformite_df = pd.concat([
    plant_df[plant_df["Resource"]=="Wind"][["Plant ID"]].merge(conformite_list_df[["Types de conformité", "Identifiant"]], how="cross"),
    plant_df[plant_df["Resource"]!="Wind"][["Plant ID"]].merge(conformite_list_df.iloc[:10,:][["Types de conformité", "Identifiant"]], how="cross")
])
conformite_df["Identifier"] = conformite_df.apply(lambda x: x["Identifiant"].replace(r"{quadrigramme}", x["Plant ID"]), axis=1)
conformite_df["Compliance"] = conformite_df["Plant ID"] + " - " + conformite_df["Types de conformité"]
conformite_df["Type"] = "Regolatory"
conformite_df = conformite_df.drop(columns=["Identifiant", "Types de conformité"])

conformite_df.rename(columns={"Plant ID": "Plant IDs"}).to_excel(r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\03.Bluepoint import data\11.regulatory_compliance.xlsx", index=False)