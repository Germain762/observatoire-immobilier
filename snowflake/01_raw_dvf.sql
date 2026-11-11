-- =====================================================================
-- 01_raw_dvf.sql — Zone RAW et périmètre
--
-- RAW.DVF_MUTATIONS reçoit les fichiers geo-dvf nationaux tels quels,
-- une année par fichier, toutes colonnes en texte : un chargement ne
-- doit jamais échouer sur un type. Le typage se fait dans CORE.
--
-- Les noms de colonnes RAW sont les nôtres (MAJUSCULES) ; le lien avec
-- l'en-tête du CSV est porté par le mapping du pipeline Fabric
-- (docs/etape-2-dvf.md). Si l'en-tête change, seul le mapping bouge.
--
-- À exécuter avec R_IMMO_ENGINEER. Relançable sans casse.
-- =====================================================================

USE ROLE R_IMMO_ENGINEER;
USE WAREHOUSE WH_IMMO_XS;

-- ---------------------------------------------------------------------
-- 1. Périmètre : les départements que CORE retient.
--    RAW est national ; pour élargir, passer ACTIF à TRUE ou ajouter
--    des lignes, puis relancer la procédure de CORE.
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS FONCIER.REF COMMENT = 'Référentiels et paramètres';

CREATE TABLE IF NOT EXISTS FONCIER.REF.PERIMETRE (
  CODE_DEPARTEMENT VARCHAR NOT NULL,
  LIBELLE          VARCHAR NOT NULL,
  ACTIF            BOOLEAN NOT NULL DEFAULT TRUE,
  CONSTRAINT PK_PERIMETRE PRIMARY KEY (CODE_DEPARTEMENT)
)
COMMENT = 'Départements traités par CORE.MUTATIONS_LOGEMENT';

MERGE INTO FONCIER.REF.PERIMETRE t
USING (
  SELECT column1 AS CODE_DEPARTEMENT, column2 AS LIBELLE
  FROM VALUES
    ('14', 'Calvados'),
    ('27', 'Eure'),
    ('50', 'Manche'),
    ('61', 'Orne'),
    ('76', 'Seine-Maritime')
) s
  ON t.CODE_DEPARTEMENT = s.CODE_DEPARTEMENT
WHEN NOT MATCHED THEN
  INSERT (CODE_DEPARTEMENT, LIBELLE, ACTIF) VALUES (s.CODE_DEPARTEMENT, s.LIBELLE, TRUE);

-- ---------------------------------------------------------------------
-- 2. Table RAW
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FONCIER.RAW.DVF_MUTATIONS (
  -- Colonnes du fichier geo-dvf
  ID_MUTATION                  VARCHAR,
  DATE_MUTATION                VARCHAR,
  NUMERO_DISPOSITION           VARCHAR,
  NATURE_MUTATION              VARCHAR,
  VALEUR_FONCIERE              VARCHAR,
  ADRESSE_NUMERO               VARCHAR,
  ADRESSE_SUFFIXE              VARCHAR,
  ADRESSE_CODE_VOIE            VARCHAR,
  ADRESSE_NOM_VOIE             VARCHAR,
  CODE_POSTAL                  VARCHAR,
  CODE_COMMUNE                 VARCHAR,
  NOM_COMMUNE                  VARCHAR,
  ANCIEN_CODE_COMMUNE          VARCHAR,
  ANCIEN_NOM_COMMUNE           VARCHAR,
  CODE_DEPARTEMENT             VARCHAR,
  ID_PARCELLE                  VARCHAR,
  ANCIEN_ID_PARCELLE           VARCHAR,
  NUMERO_VOLUME                VARCHAR,
  LOT1_NUMERO                  VARCHAR,
  LOT1_SURFACE_CARREZ          VARCHAR,
  LOT2_NUMERO                  VARCHAR,
  LOT2_SURFACE_CARREZ          VARCHAR,
  LOT3_NUMERO                  VARCHAR,
  LOT3_SURFACE_CARREZ          VARCHAR,
  LOT4_NUMERO                  VARCHAR,
  LOT4_SURFACE_CARREZ          VARCHAR,
  LOT5_NUMERO                  VARCHAR,
  LOT5_SURFACE_CARREZ          VARCHAR,
  NOMBRE_LOTS                  VARCHAR,
  CODE_TYPE_LOCAL              VARCHAR,
  TYPE_LOCAL                   VARCHAR,
  SURFACE_REELLE_BATI          VARCHAR,
  NOMBRE_PIECES_PRINCIPALES    VARCHAR,
  CODE_NATURE_CULTURE          VARCHAR,
  NATURE_CULTURE               VARCHAR,
  CODE_NATURE_CULTURE_SPECIALE VARCHAR,
  NATURE_CULTURE_SPECIALE      VARCHAR,
  SURFACE_TERRAIN              VARCHAR,
  LONGITUDE                    VARCHAR,
  LATITUDE                     VARCHAR,
  -- Métadonnées ajoutées par le pipeline
  ANNEE_FICHIER                VARCHAR,
  SOURCE_URL                   VARCHAR,
  DATE_CHARGEMENT              VARCHAR,
  ID_EXECUTION                 VARCHAR
)
COMMENT = 'DVF géolocalisées (Etalab), fichiers nationaux par année, chargés tels quels';
