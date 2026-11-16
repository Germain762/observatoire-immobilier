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

# # nb_fonctions_immo
# 
# Fonctions partagées, chargées par `%run nb_fonctions_immo`. Aucune lecture ni écriture ici.
# 
# - `normaliser_voie` : nom de voie comparable entre DVF et la BAN (majuscules, sans accents, types de voie développés, mots vides retirés).
# - `numero_voie` : numéro de voie entier (« 12 bis » → 12).
# - `cle_adresse` : commune | numéro | voie normalisée, clé du premier niveau d'appariement (étape 5).

# CELL ********************

from pyspark.sql import Column
from pyspark.sql import functions as F

ACCENTS_SRC = "ÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÑÒÓÔÕÖÙÚÛÜÝŸ"
ACCENTS_DST = "AAAAAACEEEEIIIINOOOOOUUUUYY"

# Abréviations des types de voie (DVF, saisies libres) -> forme développée (BAN)
TYPES_VOIE = {
    "R": "RUE", "AV": "AVENUE", "AVE": "AVENUE", "BD": "BOULEVARD", "BLD": "BOULEVARD",
    "CHE": "CHEMIN", "CHEM": "CHEMIN", "CHM": "CHEMIN", "CR": "CHEMIN RURAL",
    "VC": "VOIE COMMUNALE", "IMP": "IMPASSE", "PL": "PLACE", "RTE": "ROUTE",
    "ALL": "ALLEE", "CRS": "COURS", "SQ": "SQUARE", "QUA": "QUARTIER", "HAM": "HAMEAU",
    "RES": "RESIDENCE", "LOT": "LOTISSEMENT", "PAS": "PASSAGE", "QU": "QUAI",
    "SEN": "SENTIER", "FG": "FAUBOURG", "PROM": "PROMENADE", "TSSE": "TERRASSE",
    "VLA": "VILLA", "ZA": "ZONE ARTISANALE", "ZI": "ZONE INDUSTRIELLE",
    "ST": "SAINT", "STE": "SAINTE",
}
MOTS_VIDES = ["DE", "DU", "DES", "LA", "LE", "LES", "L", "D", "ET", "A", "AU", "AUX"]


def normaliser_voie(col: Column) -> Column:
    """Nom de voie normalisé, ou NULL si vide."""
    carte = F.create_map(*[F.lit(x) for paire in TYPES_VOIE.items() for x in paire])
    c = F.upper(F.trim(col))
    c = F.regexp_replace(c, "Œ", "OE")
    c = F.translate(c, ACCENTS_SRC, ACCENTS_DST)
    c = F.regexp_replace(c, "[^A-Z0-9]+", " ")
    mots = F.split(F.trim(c), " ")
    mots = F.transform(mots, lambda m: F.coalesce(carte[m], m))
    mots = F.filter(mots, lambda m: (m != "") & ~m.isin(MOTS_VIDES))
    resultat = F.array_join(mots, " ")
    return F.when(F.length(resultat) > 0, resultat)


def numero_voie(col: Column) -> Column:
    """Partie numérique en tête du numéro de voie, en entier."""
    n = F.regexp_extract(F.trim(col.cast("string")), r"^(\d+)", 1)
    return F.when(n != "", n.cast("int"))


def cle_adresse(code_commune: Column, numero: Column, voie_norm: Column) -> Column:
    """Clé commune|numéro|voie ; NULL si un des trois manque."""
    return F.when(
        code_commune.isNotNull() & numero.isNotNull() & voie_norm.isNotNull(),
        F.concat_ws("|", code_commune, numero.cast("string"), voie_norm),
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Autotest
# 
# `%run` exécute toutes les cellules : ce test tourne donc à chaque chargement, en quelques millisecondes. Si la normalisation régresse, le notebook appelant échoue avant d'écrire quoi que ce soit.

# CELL ********************

_attendus = [
    ("12 bis", "R DE LA RÉPUBLIQUE", "76540", 12, "RUE REPUBLIQUE", "76540|12|RUE REPUBLIQUE"),
    ("3", "Avenue  Jean-Jaurès", "76600", 3, "AVENUE JEAN JAURES", "76600|3|AVENUE JEAN JAURES"),
    ("", "IMP DES ŒILLETS", "14000", None, "IMPASSE OEILLETS", None),
    (None, None, "27000", None, None, None),
]
_obtenus = (
    spark.createDataFrame([a[:3] for a in _attendus], "numero string, voie string, commune string")
    .select(
        numero_voie(F.col("numero")).alias("n"),
        normaliser_voie(F.col("voie")).alias("v"),
        cle_adresse(F.col("commune"), numero_voie(F.col("numero")), normaliser_voie(F.col("voie"))).alias("c"),
    )
    .collect()
)
for _a, _o in zip(_attendus, _obtenus):
    assert (_o["n"], _o["v"], _o["c"]) == _a[3:], f"Normalisation inattendue pour {_a[:3]} : {tuple(_o)}"
print("nb_fonctions_immo chargé, autotest OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
