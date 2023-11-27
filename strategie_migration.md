Stratégie migration
===================

## Paramètrage plateforme
Ajout des types de contrats / events / milestone / portoflio

__Spec__ : 

__Fichiers__ : -
## Ajout des entreprises
Reprise extract SAP. Formater les noms en _title case_

__Fichiers__ : 
- 01.company.xlsx

## Ajout des contacts
Injecter la liste du personnel Omega + fonctions + entreprise
Récupérer la liste des contacts standards de certains presta : 
- ACR / ARD Enedis
- RTE ?
- ?


Puis, par groupe de GA : 
- faire ajouter les contacts associés à chaque parc en mettant à disposition les contacts donnés par les GA précédents
- injecter les nouveaux contacts dans Bluepoint

__Fichiers__ : 
- 02.contacts_edf_r.xlsx
- 03.contacts_externes_standard.xlsx
- 05.centrales_contacts.xlsx

## Ajout des centrales
Faire valider le fichier _plants_ par groupe de GA (et compléter au besoin)
Ajouter les contacts donnés précédemment
Injecter les centrales + emplacements avec un tag "à vérifier" et avec le portefeuille "général"

Injecter les centrales virtuelles dans le protfeuille "facturation" ?

__Fichiers__ : 
- 04.plants_solar.xlsx
- 04.plants_wind.xlsx

## Ajout des équipements / composants
Récupérer : 
- les éoliennes de RDL
- les équipements solaires de WindGA
- stockage ?
- les modèles depuis Kiwi

Donner à chaque groupe de GA : 
- la liste ses équipements principaux + modèle
- fichier excel = une feuille par centrale

__Faire valider les modèles sur le même modèle que les contacts__
Mettre à disposition le fichier pour récupérer les données autre sur l'éolien des GA -> pré-remplir avec les équipements de sécurité mais données pas terrible

## Ajout des contrats
### Contrat
Injection des contrats WindGA dans Bluepoint avec un tag "à vérifier". Faire passer les GA / CEVEA selon le type de contrat.
Tenir à jour le dashboard de converture contractuelle.
Faire ajouter le prix initial de chaque contrat

Après injection, faire supprimers les contrats d'achat en doublon par CEVEA et rattacher toutes les centrales au bon contrat.

### Grilles tarifaire

Pour les contrats gérés par la facturation, faire compléter les tarifs initiaux dans le fichier d'import Bluepoint "Rate schedule". Aide possible si on arrive à rapprocher les excel actulels avec les contrats windga et/ou Bluepoint.
Injecter les grilles par petit paquet (10 / 20 ?) et vérifier avec la facturation à chaque fois. 

Une fois les grilles injecté, mettre à jour les indices via template préparer pour cela.

Ajouter les centrales virtuelles aux grilles des contrats d'achat ?

### _Utility statements_

Récupérer les contrats d'achat et compléter le template d'import avec cette liste.


## Ajout des _compliance_
Injection des compliance communes à tous les parcs + tâches associés : 
- suivi icpe année 1 et 10
- contrôles brides fixation + eqipements pression
- OR3 ?
- fiche SEE
- PDP ?
- acoustique

Injection des conformité standard relatives aux contrats 
- fin de contrat
- valition dispo
- rapport maintenance
- rapport autres ?

Faire paramétrer les conformité de l'enviro dans le template d'import excel : 
- suivi additionnels
- entretient site
- autre ?

Faire completer les conformités autres à CEVEA ? aux GA ?

## Ajout données perf
Extract P50 / Dispo théorique etc (= _forcast_ ) de WindGA (ou RDL ?) + validation GA. Le GA doit récupérer le lien du document associé ( et le stocker au bon endroit au besoin). 

Injecter les _forcast_

Injecter les données RDL
Injecter les données WindGA (écraseront les données RDL si déjà présentes, ok car WindGA = données validées)
Injecter les données Enedis
Injecter les données RTE
Injecter les données aux gestionnaires réseaux (SRD)

## Ajout des budgets
Récupérer fichiers Monika, mettre en forme puis valider sur quelques parcs

Vérifier _char of account_ avec Aubin + Monika + DFF

Injecter budgets, un "LE3", un "BN" (changer par meilleur nom)

Pareil pour business plan. Voir pour valider les noms avec OR + DFF (+ Damien ?)

## Ajout des évènements
Récupérer les données WSP v2, formater puis valider avec les GA.
Si MCR (ou similaire) faire renseigner la machine correspondante.

Injecter les évènements avec tag "à vérifier". 
KPI remplissage :
- "has financiel impact" + pertes € et temps
- resolution notes
- root cause
- status
- lien dossier serveur (présence + format). Tout doit être stocké sur Sharepoint à ce moment là

## Pour aller plus loin
Récupérer les anciens excel wsp, compiler et faire vérifier par GA. Objectif exemple de complétion : 
	- mars -> 2022 & S2 2021
-avril -> S1 2021 & 2020
etc.

Ou bien xx event / GA / mois

Même indicateurs qu'on dessus -> implique de ranger les dossiers !

Créer les claims assurances et les associés au bon event












