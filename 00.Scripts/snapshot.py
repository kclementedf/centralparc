# %%
import os
import re

import pandas as pd

from icarus.io.windga import engine, db_session
from icarus.io.windga import (
    # CENTRALEINTERLOCUTEUR,
    # CENTRALELOCALISATION,
    # EVENEMENTS,
    # INTERLOCUTEUR,
    # LISTEEVENEMENTS,
    # LOCALISATION,
    PRODUCTIBLEREELMENSUEL,
    PRODUCTIBLEREELMENSUELDETAIL,
    CENTRALE,
    PROJET,
    CONTRAT,
    TYPEENTITEELEMENTS,
    TYPEPROJET,
    EQUIPEMENTMATERIEL,
    MATERIEL,
    # STATUT,
    # CENTRALESTATUT,
    # PROJETINTERLOCUTEUR,
    # CONTRAT_INTERLOCUTEUR,
)

export_path = r"C:\Users\kclement\EDF Renouvelables\Central Parc - 02 - Conception - 02 - Conception\04 - Migration données\01.Snapshot"

with engine.begin() as con:
    query = db_session.query(CENTRALE).filter(CENTRALE.CODE_PI != None)
    centrale_df = pd.read_sql(query.statement, con)
    centrale_df.to_parquet(os.path.join(export_path, "centrale.parquet"))

    query = db_session.query(PROJET)
    projet_df = pd.read_sql(query.statement, con)
    projet_df.to_parquet(os.path.join(export_path, "projet.parquet"))

    query = db_session.query(TYPEENTITEELEMENTS)
    type_entite_element_df = pd.read_sql(query.statement, con)
    type_entite_element_df.to_parquet(os.path.join(
        export_path, "type_entite_element.parquet"))

    query = db_session.query(TYPEPROJET)
    type_projet_df = pd.read_sql(query.statement, con)
    type_projet_df.to_parquet(os.path.join(export_path, "type_projet.parquet"))

    query = db_session.query(EQUIPEMENTMATERIEL)
    equipement_df = pd.read_sql(query.statement, con)
    equipement_df.to_parquet(os.path.join(export_path, "equipement.parquet"))

    query = db_session.query(MATERIEL)
    materiel_df = pd.read_sql(query.statement, con)
    materiel_df.to_parquet(os.path.join(export_path, "materiel.parquet"))

    query = db_session.query(CONTRAT)
    contrat_df = pd.read_sql(query.statement, con)
    contrat_df.to_parquet(os.path.join(export_path, "contrat.parquet"))

    # query = db_session.query(INTERLOCUTEUR)
    # interlocuteur_df = pd.read_sql(query.statement, con)
    # interlocuteur_df.to_parquet(os.path.join(
    #     export_path, "interlocuteur.parquet"))

    # query = db_session.query(CENTRALEINTERLOCUTEUR)
    # interlocuteur_df = pd.read_sql(query.statement, con)
    # interlocuteur_df.to_parquet(os.path.join(
    #     export_path, "centrale_interlocuteur.parquet"))

    query = db_session.query(PRODUCTIBLEREELMENSUEL)
    productible_reel_mensuel_df = pd.read_sql(query.statement, con)
    productible_reel_mensuel_df.to_parquet(os.path.join(export_path, "productible_reel_mensuel.parquet"))

    query = db_session.query(PRODUCTIBLEREELMENSUELDETAIL)
    productible_reel_mensuel_detail_df = pd.read_sql(query.statement, con)
    productible_reel_mensuel_detail_df.to_parquet(os.path.join(export_path, "productible_reel_mensuel_detail.parquet"))

    # query = db_session.query(STATUT)
    # statut_df = pd.read_sql(query.statement, con)
    # statut_df.to_parquet(os.path.join(export_path, "statut.parquet"))

    # query = db_session.query(EVENEMENTS)
    # evenements_df = pd.read_sql(query.statement, con)
    # evenements_df.to_parquet(os.path.join(export_path, "evenements.parquet"))

    # query = db_session.query(LISTEEVENEMENTS)
    # liste_evenements_df = pd.read_sql(query.statement, con)
    # liste_evenements_df.to_parquet(os.path.join(
    #     export_path, "liste_evenements.parquet"))

    # query = db_session.query(LOCALISATION)
    # localisation_df = pd.read_sql(query.statement, con)
    # localisation_df.to_parquet(os.path.join(
    #     export_path, "localisation.parquet"))

    # query = db_session.query(CENTRALELOCALISATION)
    # centrale_localisation_df = pd.read_sql(query.statement, con)
    # centrale_localisation_df.to_parquet(os.path.join(
    #     export_path, "centrale_localisation.parquet"))

    # query = db_session.query(PROJETINTERLOCUTEUR)
    # projet_interlocuteur_df = pd.read_sql(query.statement, con)
    # projet_interlocuteur_df.to_parquet(os.path.join(
    #     export_path, "projet_interlocuteur.parquet"))


    # query = db_session.query(CENTRALEINTERLOCUTEUR)
    # centrale_interlocuteur_df = pd.read_sql(query.statement, con)
    # centrale_interlocuteur_df.to_parquet(os.path.join(
    #     export_path, "centrale_interlocuteur.parquet"))

# %%
