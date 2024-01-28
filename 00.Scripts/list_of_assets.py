import pandas as pd

list_of_asset_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\02 - Ateliers\Manager GA\Copie de List of assets_FR_20200701.xlsx"

excel_reader = pd.ExcelFile(list_of_asset_path)

data = []
for sheet_name in excel_reader.sheet_names:
    df = pd.read_excel(
        list_of_asset_path,
        sheet_name=sheet_name
        )
    df = df.iloc[3:,3:8]
    df.columns = ["Nom List of asset", "vide_1", "Engagement", "vide_2", "Commentaire"]
    df = df.dropna(subset=["Nom List of asset"])
    df["quadrigramme"] = sheet_name
    data.append(df)

df = pd.concat(data)

correspondance_df = pd.read_excel(
    r"C:\Users\kclement\EDF Renouvelables\Central Parc - 03 - Réalisation - 03 - Réalisation\02.Migration données\04.Probleme\champs_list_of_asset.xlsx",
    sheet_name="Sheet1"
    )

df = df.merge(correspondance_df, how="left", left_on="Nom List of asset", right_on="Nom List of asset")

df.to_excel(
    r"C:\Users\kclement\EDF Renouvelables\Central Parc - 03 - Réalisation - 03 - Réalisation\02.Migration données\03.Import data\01.Brut\list_of_asset.xlsx",
    index=False
)