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

# # nb_silver_dpe
# 
# Construit `silver.dpe` depuis `bronze.dpe` : un DPE en vigueur par numéro, logements seulement, typé, géolocalisé, avec une clé d'adresse pour l'appariement de l'étape 5.
# 
# Règles, dans l'ordre :
# 1. un DPE cité dans `numero_dpe_remplace` d'un autre DPE est périmé ;
# 2. un seul enregistrement par `numero_dpe` (le plus récemment modifié) ;
# 3. maisons et appartements seulement (les DPE d'immeuble sont exclus) ;
# 4. les DPE d'appartement générés à partir d'un DPE d'immeuble sont gardés et signalés ;
# 5. les valeurs hors bornes sont gardées et signalées par `est_plausible`.
# 
# Lakehouse par défaut : `lh_immo`.

# CELL ********************

%run nb_fonctions_immo

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

from pyspark.sql import Window
from pyspark.sql import functions as F

bronze = spark.read.table("bronze.dpe").cache()
n_bronze = bronze.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Étapes 1 à 3 : DPE en vigueur, uniques, logements

# CELL ********************

# 1. DPE remplacés. L'anti-jointure gère aussi les chaînes A -> B -> C :
#    A et B sont chacun cités comme remplacés, seul C reste.
remplaces = (
    bronze.where(F.col("numero_dpe_remplace").isNotNull())
    .select(F.col("numero_dpe_remplace").alias("numero_dpe"))
    .distinct()
)
en_vigueur = bronze.join(remplaces, "numero_dpe", "left_anti")

# 2. Un enregistrement par numéro
ordre = Window.partitionBy("numero_dpe").orderBy(
    F.col("date_derniere_modification_dpe").desc_nulls_last(),
    F.col("date_chargement").desc(),
)
uniques = en_vigueur.withColumn("_rang", F.row_number().over(ordre)).where("_rang = 1").drop("_rang")

# 3. Logements
logements = uniques.where(F.col("type_batiment").isin("maison", "appartement"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Typage et colonnes d'appariement
# 
# - Dates au format ISO dans l'API : `to_date` sans format explicite.
# - `_geopoint` vaut « latitude,longitude » en WGS84, le même système que les coordonnées DVF.
# - `type_local` reprend les libellés DVF (« Maison », « Appartement »).

# CELL ********************

geo = F.split(F.col("_geopoint"), ",")
date_etab = F.to_date("date_etablissement_dpe")
surface = F.col("surface_habitable_logement")
etiquettes = ["A", "B", "C", "D", "E", "F", "G"]

silver = (
    logements.select(
        "numero_dpe",
        date_etab.alias("date_etablissement"),
        F.to_date("date_reception_dpe").alias("date_reception"),
        F.to_date("date_derniere_modification_dpe").alias("date_derniere_modification"),
        F.to_date("date_fin_validite_dpe").alias("date_fin_validite"),
        F.upper("etiquette_dpe").alias("etiquette_dpe"),
        F.upper("etiquette_ges").alias("etiquette_ges"),
        F.col("conso_5_usages_par_m2_ep").alias("conso_ep_m2"),
        F.col("emission_ges_5_usages_par_m2").alias("ges_m2"),
        F.initcap("type_batiment").alias("type_local"),
        surface.alias("surface_habitable"),
        "annee_construction",
        "periode_construction",
        "typologie_logement",
        "numero_etage_appartement",
        "type_energie_principale_chauffage",
        "methode_application_dpe",
        F.coalesce(
            F.col("methode_application_dpe").contains("généré à partir des données DPE immeuble"),
            F.lit(False),
        ).alias("est_genere_immeuble"),
        "numero_dpe_immeuble_associe",
        F.col("code_insee_ban").alias("code_commune"),
        F.col("code_postal_ban").alias("code_postal"),
        F.col("code_departement_ban").alias("code_departement"),
        F.col("nom_commune_ban").alias("nom_commune"),
        "adresse_ban",
        "complement_adresse_logement",
        "identifiant_ban",
        "score_ban",
        "statut_geocodage",
        numero_voie(F.col("numero_voie_ban")).alias("numero_voie"),
        normaliser_voie(F.col("nom_rue_ban")).alias("voie_norm"),
        geo.getItem(0).cast("double").alias("latitude"),
        geo.getItem(1).cast("double").alias("longitude"),
        "id_rnb",
        "id_execution",
    )
    .withColumn("cle_adresse", cle_adresse(F.col("code_commune"), F.col("numero_voie"), F.col("voie_norm")))
    .withColumn(
        "est_plausible",
        F.col("surface_habitable").between(9, 1000)
        & F.col("etiquette_dpe").isin(etiquettes)
        & (F.col("date_etablissement") >= F.lit("2021-07-01").cast("date")),
    )
    .withColumn("date_maj_silver", F.current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Contrôles

# CELL ********************

silver = silver.cache()
n_silver = silver.count()

# Bloquants
doublons = silver.groupBy("numero_dpe").count().where("count > 1").count()
sans_numero = silver.where(F.col("numero_dpe").isNull()).count()
sans_departement = silver.where(F.col("code_departement").isNull()).count()
assert doublons == 0, f"{doublons} numero_dpe en double"
assert sans_numero == 0, f"{sans_numero} DPE sans numéro"
assert sans_departement == 0, f"{sans_departement} DPE sans département"
assert 0 < n_silver <= n_bronze, f"Volume silver incohérent : {n_silver} pour {n_bronze} en bronze"

# Entonnoir bronze -> silver
entonnoir = [
    ("bronze", n_bronze),
    ("en vigueur (remplacés retirés)", en_vigueur.count()),
    ("un par numéro", uniques.count()),
    ("maisons et appartements", n_silver),
    ("plausibles", silver.where("est_plausible").count()),
    ("avec clé d'adresse", silver.where(F.col("cle_adresse").isNotNull()).count()),
    ("géolocalisés", silver.where(F.col("latitude").isNotNull()).count()),
]
display(spark.createDataFrame(entonnoir, "etape string, dpe long"))

# Par département, type et origine du DPE
display(
    silver.groupBy("code_departement", "type_local", "est_genere_immeuble")
    .agg(
        F.count("*").alias("dpe"),
        F.round(F.avg(F.col("est_plausible").cast("int")) * 100, 1).alias("pct_plausible"),
        F.round(F.avg(F.col("cle_adresse").isNotNull().cast("int")) * 100, 1).alias("pct_cle_adresse"),
    )
    .orderBy("code_departement", "type_local", "est_genere_immeuble")
)

# Répartition des étiquettes (DPE plausibles)
display(
    silver.where("est_plausible")
    .groupBy("type_local")
    .pivot("etiquette_dpe", etiquettes)
    .count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Écriture de `silver.dpe`
# 
# Reconstruction complète : la table est petite à l'échelle d'une région, et une reconstruction rend le notebook idempotent.

# CELL ********************

(silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("code_departement")
    .saveAsTable("silver.dpe"))

bronze.unpersist()
silver.unpersist()

resume = {"bronze": n_bronze, "silver": n_silver}
print(resume)
notebookutils.notebook.exit(json.dumps(resume))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
