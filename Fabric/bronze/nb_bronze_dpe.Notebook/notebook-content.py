# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "989d0e04-6018-4068-b46f-b89e9b9eb930",
# META       "default_lakehouse_name": "lh_immo",
# META       "default_lakehouse_workspace_id": "4699b961-c47b-472e-a5e4-a062b6212736",
# META       "known_lakehouses": [
# META         {
# META           "id": "989d0e04-6018-4068-b46f-b89e9b9eb930"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_bronze_dpe
# 
# Télécharge les DPE logements existants (ADEME, jeu `dpe03existant`) département par département, dépose les pages brutes dans `Files/landing/dpe/`, puis les charge dans `bronze.dpe`.
# 
# - **Périmètre** : par défaut, les départements présents dans `bronze.mutations_logement` (le miroir Snowflake). Le périmètre DPE suit donc automatiquement celui des ventes.
# - **Rechargement complet par département** : `replaceWhere` remplace uniquement les départements chargés.
# - **Bloquant** : un département n'est écrit que si le nombre de lignes lues est égal au `total` annoncé par l'API.
# 
# Lakehouse par défaut : `lh_immo`.

# PARAMETERS CELL ********************

# Départements séparés par des virgules ; vide = départements présents dans bronze.mutations_logement
p_departements = ""
p_taille_page = 10000
p_runs_conserves = 3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import gzip
import json
import os
import shutil
import time
from datetime import datetime, timezone

import requests
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

URL_API = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"
DOSSIER_LANDING = "/lakehouse/default/Files/landing/dpe"   # chemin local (montage du lakehouse)
CHEMIN_SPARK = "Files/landing/dpe"                          # même dossier vu par Spark
TABLE_BRONZE = "bronze.dpe"
TABLE_JOURNAL = "bronze.dpe_chargement"

# Champs demandés à l'API : le jeu en compte plus de 200, on ne garde que l'utile
# (moins de volume, donc moins de temps sous la limite de débit anonyme).
TEXTE = ["numero_dpe", "numero_dpe_remplace", "numero_dpe_immeuble_associe",
         "date_etablissement_dpe", "date_reception_dpe", "date_derniere_modification_dpe",
         "date_fin_validite_dpe", "methode_application_dpe", "type_batiment",
         "etiquette_dpe", "etiquette_ges", "periode_construction", "typologie_logement",
         "type_energie_principale_chauffage", "adresse_ban", "numero_voie_ban", "nom_rue_ban",
         "code_postal_ban", "code_insee_ban", "nom_commune_ban", "code_departement_ban",
         "identifiant_ban", "statut_geocodage", "complement_adresse_logement",
         "id_rnb", "provenance_id_rnb", "_geopoint"]
DECIMAL = ["version_dpe", "conso_5_usages_par_m2_ep", "emission_ges_5_usages_par_m2",
           "surface_habitable_logement", "score_ban"]
ENTIER = ["annee_construction", "nombre_niveau_logement", "numero_etage_appartement"]
CHAMPS = TEXTE + DECIMAL + ENTIER

SCHEMA = StructType(
    [StructField(c, StringType()) for c in TEXTE]
    + [StructField(c, DoubleType()) for c in DECIMAL]
    + [StructField(c, LongType()) for c in ENTIER]
    + [StructField("_corrupt_record", StringType())]
)

id_execution = datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")
print(id_execution)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Périmètre

# CELL ********************

if p_departements.strip():
    departements = sorted({d.strip() for d in p_departements.split(",") if d.strip()})
else:
    departements = sorted(
        r["CODE_DEPARTEMENT"]
        for r in spark.read.table("bronze.mutations_logement").select("CODE_DEPARTEMENT").distinct().collect()
    )
assert departements, "Aucun département à charger"
print("Départements :", ", ".join(departements))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Téléchargement
# 
# Une page de l'API = un fichier `page_NNNNN.json.gz` (une ligne JSON par DPE). La pagination suit le lien `next`, qui porte un curseur `after` : on ne dépend pas de la limite de 10 000 résultats de la pagination classique.
# 
# Limites de l'API en accès anonyme : 600 requêtes par minute, environ 500 ko/s, et un temps de calcul plafonné. Une réponse 429 ou 5xx est retentée avec une attente croissante.

# CELL ********************

session = requests.Session()
session.headers.update({"User-Agent": "projet-valeur-verte (apprentissage Microsoft Fabric)"})


def appeler(url, params=None, essais=6):
    for essai in range(essais):
        try:
            r = session.get(url, params=params, timeout=180)
        except requests.RequestException as e:
            attente = min(2 ** essai * 5, 120)
            print(f"    erreur réseau ({e.__class__.__name__}), nouvel essai dans {attente} s")
            time.sleep(attente)
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            attente = int(r.headers.get("Retry-After", 0) or 0) or min(2 ** essai * 5, 120)
            print(f"    HTTP {r.status_code}, nouvel essai dans {attente} s")
            time.sleep(attente)
            continue
        r.raise_for_status()
    raise RuntimeError(f"Échec après {essais} essais : {url}")


def telecharger_departement(dep, dossier):
    os.makedirs(dossier, exist_ok=True)
    params = {
        "size": p_taille_page,
        "select": ",".join(CHAMPS),
        "qs": f'code_departement_ban:"{dep}"',
    }
    reponse = appeler(URL_API, params)
    total = reponse["total"]
    pages = lignes = 0
    while True:
        resultats = reponse.get("results", [])
        if resultats:
            pages += 1
            with gzip.open(f"{dossier}/page_{pages:05d}.json.gz", "wt", encoding="utf-8") as f:
                for ligne in resultats:
                    f.write(json.dumps(ligne, ensure_ascii=False) + "\n")
            lignes += len(resultats)
            if pages % 10 == 0:
                print(f"    {lignes:,} / {total:,}")
        suivant = reponse.get("next")
        if not suivant or not resultats:
            break
        reponse = appeler(suivant)
    return total, lignes, pages


journal = []
for dep in departements:
    debut = datetime.now(timezone.utc)
    print(f"Département {dep}")
    total, lignes, pages = telecharger_departement(dep, f"{DOSSIER_LANDING}/{id_execution}/dep_{dep}")
    fin = datetime.now(timezone.utc)
    print(f"  {lignes:,} lignes en {pages} pages, total API {total:,}, {(fin - debut).seconds} s")
    journal.append({"code_departement": dep, "total_api": total, "lignes_telechargees": lignes,
                    "pages": pages, "debut": debut, "fin": fin})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Lecture Spark et contrôles
# 
# `spark.read.json` avec un schéma explicite : les champs absents d'une ligne (l'API omet les valeurs nulles) deviennent NULL, et une ligne illisible atterrit dans `_corrupt_record` au lieu de faire échouer la lecture.

# CELL ********************

brut = (
    spark.read.schema(SCHEMA)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(f"{CHEMIN_SPARK}/{id_execution}/dep_*/*.json.gz")
    .withColumn("fichier_source", F.col("_metadata.file_path"))
    .cache()   # requis par Spark pour filtrer sur _corrupt_record
)

corrompues = brut.where(F.col("_corrupt_record").isNotNull()).count()
assert corrompues == 0, f"{corrompues} lignes JSON illisibles"

par_dep = {r["code_departement_ban"]: r["n"]
           for r in brut.groupBy("code_departement_ban").agg(F.count("*").alias("n")).collect()}

ecarts = []
for j in journal:
    j["lignes_lues"] = par_dep.get(j["code_departement"], 0)
    if not (j["lignes_lues"] == j["lignes_telechargees"] == j["total_api"]):
        ecarts.append(j)
assert not ecarts, (
    "Écart entre l'API et les lignes lues (le jeu a peut-être été mis à jour pendant le "
    f"téléchargement ; relancer) : {ecarts}"
)

doublons = brut.groupBy("numero_dpe").count().where("count > 1").count()
if doublons:
    print(f"Avertissement : {doublons} numero_dpe en double dans le brut (dédoublonnés en silver)")
print("Contrôles OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Écriture de `bronze.dpe`
# 
# Partitionnement par département, remplacement des seuls départements chargés (`replaceWhere`). Les colonnes restent celles de l'API, sans transformation.

# CELL ********************

bronze = (
    brut.drop("_corrupt_record")
    .withColumn("id_execution", F.lit(id_execution))
    .withColumn("date_chargement", F.current_timestamp())
)

liste = ", ".join(f"'{d}'" for d in departements)
ecriture = bronze.write.format("delta").mode("overwrite").partitionBy("code_departement_ban")
if spark.catalog.tableExists(TABLE_BRONZE):
    ecriture = ecriture.option("replaceWhere", f"code_departement_ban IN ({liste})")
ecriture.saveAsTable(TABLE_BRONZE)

(spark.createDataFrame(journal)
    .withColumn("id_execution", F.lit(id_execution))
    .write.format("delta").mode("append").saveAsTable(TABLE_JOURNAL))

brut.unpersist()
display(spark.read.table(TABLE_BRONZE).groupBy("code_departement_ban").count().orderBy("code_departement_ban"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Nettoyage du dossier landing
# 
# On garde les `p_runs_conserves` dernières exécutions pour pouvoir rejouer un chargement sans rappeler l'API.

# CELL ********************

runs = sorted(d for d in os.listdir(DOSSIER_LANDING) if d.startswith("run_"))
for ancien in runs[:-p_runs_conserves]:
    shutil.rmtree(f"{DOSSIER_LANDING}/{ancien}")
    print("Supprimé :", ancien)

resume = {"id_execution": id_execution,
          "departements": departements,
          "lignes": sum(j["lignes_lues"] for j in journal)}
print(resume)
notebookutils.notebook.exit(json.dumps(resume))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
