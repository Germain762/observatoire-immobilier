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

# CELL ********************

from pyspark.sql import functions as F

df = spark.read.table("lh_immo.bronze.mutations_logement")
df.printSchema()
print(f"{df.count():,} lignes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

controle = (
    df.groupBy("CODE_DEPARTEMENT", "ANNEE")
      .agg(
          F.count("*").alias("ventes"),
          F.sum(F.col("EST_PLAUSIBLE").cast("int")).alias("plausibles"),
          F.percentile_approx(
              F.when(F.col("EST_PLAUSIBLE"), F.col("PRIX_M2")), 0.5
          ).alias("prix_m2_median"),
      )
      .orderBy("CODE_DEPARTEMENT", "ANNEE")
)
display(controle)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Bloquant : ce que le Mirroring ou la construction de CORE ne doivent jamais produire
doublons = df.groupBy("ID_VENTE").count().filter("count > 1").count()
nuls = df.filter(
    F.col("PRIX").isNull() | F.col("SURFACE_BATI").isNull() | F.col("PRIX_M2").isNull()
).count()
drapeau_faux = df.filter(
    F.col("EST_PLAUSIBLE")
    & ~(F.col("PRIX_M2").between(300, 30000) & F.col("SURFACE_BATI").between(9, 1000))
).count()

assert doublons == 0, f"{doublons} ID_VENTE en double"
assert nuls == 0, f"{nuls} lignes avec prix, surface ou prix au m² nul (NULL)"
assert drapeau_faux == 0, f"{drapeau_faux} lignes marquées plausibles hors bornes"

# Avertissement : ventes symboliques dont le prix au m² s'arrondit à 0,00 €.
# Elles restent dans la table, EST_PLAUSIBLE les écarte des analyses.
prix_zero = df.filter(F.col("PRIX_M2") <= 0)
nb_zero = prix_zero.count()
if nb_zero:
    print(f"Avertissement : {nb_zero} vente(s) à prix au m² nul après arrondi")
    display(prix_zero.select("ID_VENTE", "DATE_VENTE", "NOM_COMMUNE", "TYPE_LOCAL",
                             "PRIX", "SURFACE_BATI", "PRIX_M2", "EST_PLAUSIBLE"))

print("Contrôles OK —", df.agg(F.max("DATE_MAJ")).first()[0], "= dernière mise à jour Snowflake")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
