#%%
from uploader import BluepointUploader
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

#%%
proxies = {
    "http": "frp-wsa-01.eu.edfencorp.net:8080",
    "https": "frp-wsa-01.eu.edfencorp.net:8080"
}

proxies = None

verify = False

kiwi_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\01.Snapshot\kiwi.xlsx"

# bp_bulk_importer = BluepointUploader(
#     environment="prod",
#     verify=verify,
#     proxies=proxies
#     )

component_type = {
    "Eolienne": 20949
}

kiwi_to_bluepoint=  {
    "component_type": "component_type",
    "Model ID": "Model_ID",
    "Model Name": "name",
    "Platform Name": "Plateforme",
    "Version Name": "Version",
    "Nominal Power": "Puissance_(MW)",
    "Rotor Diameter": "Diamètre_(m)",
    "Hauteur_moyeu_(m)": "Hauteur_moyeu_(m)",
    "Company Name": "manufacturer",
}

#%%
bp_bulk_importer = BluepointUploader(
    environment="prod",
    username="kevin.clement@edf-re.fr",
    password="V96#7UjS!Fvn#sS&W!WA",
    verify=verify,
    proxies=proxies
    )

#%%
from bs4 import BeautifulSoup
csrf_token_url = "https://www.bluepoint.io/components/component_types/models/new/?next=%2Fcomponents%2Fcomponent_types%2Fmodels%2F"
r = bp_bulk_importer.s.get(
    csrf_token_url,
    verify=bp_bulk_importer.verify,
    proxies=bp_bulk_importer.proxies
)
soup = BeautifulSoup(r.text, 'html.parser')

manufacturer = {elem.text: int(elem.get("value")) for elem in soup.find(id="id_manufacturer").find_all("option") 
 if elem.get("value") != ""}


#%%
df = pd.read_excel(
    kiwi_path,
    sheet_name="model WTG",

)

# df["Model Name"] = df["Model Name"].str.split("(").str[0]
df = df.rename(columns=kiwi_to_bluepoint)
df = df[df["manufacturer"].isin(list(manufacturer.keys()))]
df["manufacturer"] = df["manufacturer"].replace(manufacturer)
df["Hauteur_moyeu_(m)"] = ""
df["component_type"] = component_type["Eolienne"]
df["Puissance_(MW)"] = df["Puissance_(MW)"].str.replace(",", ".").astype(float)
df["Diamètre_(m)"] = df["Diamètre_(m)"].str.replace(",", ".").astype(float)
df["Version"] = df["Version"].fillna("")
data = df[list(kiwi_to_bluepoint.values())].to_dict(orient="records")

# failed = bp_bulk_importer.add_multiple_models(data)