-- =====================================================================
-- 02_core_mutations_logement.sql — Ventes à un seul logement
--
-- CORE.MUTATIONS_LOGEMENT : une ligne par vente d'une maison ou d'un
-- appartement, typée, avec prix au m². C'est la table que Fabric
-- réplique par Mirroring (étape 3).
--
-- Règles (voir README, section Décisions) :
--   - nature de mutation « Vente » uniquement (pas de VEFA, d'échange,
--     d'adjudication...) ;
--   - exactement un logement par mutation, aucun local professionnel,
--     une seule valeur foncière ; les dépendances sont tolérées et comptées ;
--   - prix au m² = valeur foncière / surface réelle bâtie du logement ;
--   - les valeurs extrêmes sont gardées mais signalées (EST_PLAUSIBLE).
--
-- Clé : ID_VENTE, empreinte des attributs de la vente, plutôt que
-- id_mutation, qui n'est pas garanti stable d'une publication à l'autre.
--
-- À exécuter avec R_IMMO_ENGINEER. Relançable sans casse.
-- =====================================================================

USE ROLE R_IMMO_ENGINEER;
USE WAREHOUSE WH_IMMO_XS;

-- ---------------------------------------------------------------------
-- 1. Table cible
--    CHANGE_TRACKING = TRUE : obligatoire pour que le rôle du Mirroring,
--    qui ne possède pas la table, puisse créer son stream.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FONCIER.CORE.MUTATIONS_LOGEMENT (
  ID_VENTE          VARCHAR       NOT NULL,
  ID_MUTATION       VARCHAR,
  DATE_VENTE        DATE          NOT NULL,
  ANNEE             NUMBER(4,0)   NOT NULL,
  CODE_DEPARTEMENT  VARCHAR       NOT NULL,
  CODE_COMMUNE      VARCHAR       NOT NULL,
  NOM_COMMUNE       VARCHAR,
  CODE_POSTAL       VARCHAR,
  ADRESSE           VARCHAR,
  ID_PARCELLE       VARCHAR,
  TYPE_LOCAL        VARCHAR       NOT NULL,
  SURFACE_BATI      NUMBER(10,2)  NOT NULL,
  SURFACE_CARREZ    NUMBER(10,2),
  NB_PIECES         NUMBER(5,0),
  SURFACE_TERRAIN   NUMBER(14,2),
  NB_DEPENDANCES    NUMBER(5,0)   NOT NULL,
  PRIX              NUMBER(15,2)  NOT NULL,
  PRIX_M2           NUMBER(12,2)  NOT NULL,
  LONGITUDE         FLOAT,
  LATITUDE          FLOAT,
  EST_PLAUSIBLE     BOOLEAN       NOT NULL,
  EST_GEOLOCALISE   BOOLEAN       NOT NULL,
  ANNEE_FICHIER     VARCHAR,
  HASH_CONTENU      VARCHAR       NOT NULL,
  DATE_MAJ          TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT PK_MUTATIONS_LOGEMENT PRIMARY KEY (ID_VENTE)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Ventes DVF à un seul logement (maison ou appartement), périmètre REF.PERIMETRE';

-- ---------------------------------------------------------------------
-- 2. Procédure de construction
--    Recalcule l'ensemble depuis RAW, puis applique seulement les
--    différences (MERGE + DELETE) : une relance sans changement de RAW
--    ne modifie aucune ligne, donc ne génère aucun flux CDC.
--
--    Garde-fou : si la source calculée fait moins de la moitié de la
--    cible actuelle, rien n'est modifié et la procédure lève une erreur.
--    Pour réduire volontairement le périmètre : CALL ...(TRUE).
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE FONCIER.CORE.SP_BUILD_MUTATIONS_LOGEMENT(FORCER BOOLEAN DEFAULT FALSE)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Construit CORE.MUTATIONS_LOGEMENT depuis RAW.DVF_MUTATIONS'
AS
$$
DECLARE
  nb_source       INTEGER DEFAULT 0;
  nb_cible_avant  INTEGER DEFAULT 0;
  nb_merge        INTEGER DEFAULT 0;
  nb_supprimes    INTEGER DEFAULT 0;
  volume_anormal  EXCEPTION (-20001, 'Volume source anormal : CORE.MUTATIONS_LOGEMENT non modifiée. Relancer avec FORCER = TRUE si la baisse est voulue.');
BEGIN

  CREATE OR REPLACE TEMPORARY TABLE FONCIER.RAW.TMP_MUTATIONS_LOGEMENT AS
  WITH lignes AS (
    -- Une ligne geo-dvf typée, limitée aux ventes du périmètre
    SELECT
      r.ID_MUTATION,
      TRY_TO_DATE(r.DATE_MUTATION, 'YYYY-MM-DD')            AS DATE_VENTE,
      TRY_TO_NUMBER(r.VALEUR_FONCIERE, 15, 2)                AS VALEUR,
      r.CODE_DEPARTEMENT,
      r.CODE_COMMUNE,
      r.NOM_COMMUNE,
      r.CODE_POSTAL,
      ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(
        NULLIF(TRIM(r.ADRESSE_NUMERO), ''),
        NULLIF(TRIM(r.ADRESSE_SUFFIXE), ''),
        NULLIF(TRIM(r.ADRESSE_NOM_VOIE), '')), ' ')          AS ADRESSE,
      r.ID_PARCELLE,
      r.TYPE_LOCAL,
      TRY_TO_NUMBER(r.SURFACE_REELLE_BATI, 10, 2)            AS SURFACE_BATI,
      TRY_TO_NUMBER(r.NOMBRE_PIECES_PRINCIPALES)             AS NB_PIECES,
      r.CODE_NATURE_CULTURE,
      r.CODE_NATURE_CULTURE_SPECIALE,
      TRY_TO_NUMBER(r.SURFACE_TERRAIN, 14, 2)                AS SURFACE_TERRAIN,
      r.LOT1_NUMERO,
      r.NUMERO_VOLUME,
      COALESCE(TRY_TO_NUMBER(r.LOT1_SURFACE_CARREZ, 10, 2), 0)
        + COALESCE(TRY_TO_NUMBER(r.LOT2_SURFACE_CARREZ, 10, 2), 0)
        + COALESCE(TRY_TO_NUMBER(r.LOT3_SURFACE_CARREZ, 10, 2), 0)
        + COALESCE(TRY_TO_NUMBER(r.LOT4_SURFACE_CARREZ, 10, 2), 0)
        + COALESCE(TRY_TO_NUMBER(r.LOT5_SURFACE_CARREZ, 10, 2), 0) AS CARREZ_TOTAL,
      TRY_TO_DOUBLE(r.LONGITUDE)                             AS LONGITUDE,
      TRY_TO_DOUBLE(r.LATITUDE)                              AS LATITUDE,
      r.ANNEE_FICHIER
    FROM FONCIER.RAW.DVF_MUTATIONS r
    JOIN FONCIER.REF.PERIMETRE p
      ON p.CODE_DEPARTEMENT = r.CODE_DEPARTEMENT
     AND p.ACTIF
    WHERE r.NATURE_MUTATION = 'Vente'
  ),
  profil AS (
    -- Ce que contient chaque mutation. Un même local apparaît sur
    -- plusieurs lignes quand la parcelle a plusieurs natures de culture :
    -- on compte donc des locaux distincts, pas des lignes.
    SELECT
      ID_MUTATION,
      COUNT(DISTINCT IFF(TYPE_LOCAL IN ('Maison', 'Appartement'),
        CONCAT_WS('|', COALESCE(ID_PARCELLE, ''), TYPE_LOCAL,
                       COALESCE(TO_VARCHAR(SURFACE_BATI), ''),
                       COALESCE(TO_VARCHAR(NB_PIECES), ''),
                       COALESCE(LOT1_NUMERO, '')), NULL))        AS NB_LOGEMENTS,
      COUNT_IF(TYPE_LOCAL LIKE 'Local%')                         AS NB_LIGNES_LOCAL_PRO,
      COUNT(DISTINCT IFF(TYPE_LOCAL = 'Dépendance',
        CONCAT_WS('|', COALESCE(ID_PARCELLE, ''),
                       COALESCE(LOT1_NUMERO, ''),
                       COALESCE(NUMERO_VOLUME, ''),
                       COALESCE(TO_VARCHAR(SURFACE_BATI), '')), NULL)) AS NB_DEPENDANCES,
      COUNT(DISTINCT VALEUR)                                     AS NB_VALEURS,
      MIN(VALEUR)                                                AS VALEUR
    FROM lignes
    GROUP BY ID_MUTATION
  ),
  terrain AS (
    -- Surface de terrain : une fois par parcelle et nature de culture
    SELECT ID_MUTATION, SUM(SURFACE_TERRAIN) AS SURFACE_TERRAIN
    FROM (
      SELECT DISTINCT ID_MUTATION, ID_PARCELLE, CODE_NATURE_CULTURE,
                      CODE_NATURE_CULTURE_SPECIALE, SURFACE_TERRAIN
      FROM lignes
      WHERE SURFACE_TERRAIN IS NOT NULL
    )
    GROUP BY ID_MUTATION
  ),
  logement AS (
    -- La ligne du logement (la plus grande surface si doublons de culture)
    SELECT *
    FROM lignes
    WHERE TYPE_LOCAL IN ('Maison', 'Appartement')
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ID_MUTATION
      ORDER BY SURFACE_BATI DESC NULLS LAST, ID_PARCELLE) = 1
  ),
  ventes AS (
    SELECT
      l.ID_MUTATION,
      l.DATE_VENTE,
      YEAR(l.DATE_VENTE)                                         AS ANNEE,
      l.CODE_DEPARTEMENT,
      l.CODE_COMMUNE,
      l.NOM_COMMUNE,
      l.CODE_POSTAL,
      l.ADRESSE,
      l.ID_PARCELLE,
      l.TYPE_LOCAL,
      l.SURFACE_BATI,
      IFF(l.TYPE_LOCAL = 'Appartement' AND l.CARREZ_TOTAL > 0,
          l.CARREZ_TOTAL, NULL)                                  AS SURFACE_CARREZ,
      l.NB_PIECES,
      t.SURFACE_TERRAIN,
      p.NB_DEPENDANCES,
      p.VALEUR                                                   AS PRIX,
      ROUND(p.VALEUR / l.SURFACE_BATI, 2)                        AS PRIX_M2,
      l.LONGITUDE,
      l.LATITUDE,
      l.ANNEE_FICHIER,
      l.LOT1_NUMERO
    FROM logement l
    JOIN profil p        ON p.ID_MUTATION = l.ID_MUTATION
    LEFT JOIN terrain t  ON t.ID_MUTATION = l.ID_MUTATION
    WHERE p.NB_LOGEMENTS = 1
      AND p.NB_LIGNES_LOCAL_PRO = 0
      AND p.NB_VALEURS = 1
      AND p.VALEUR > 0
      AND l.SURFACE_BATI > 0
      AND l.DATE_VENTE IS NOT NULL
      AND l.CODE_COMMUNE IS NOT NULL
  )
  SELECT
    SHA2(CONCAT_WS('|',
      TO_VARCHAR(DATE_VENTE), TO_VARCHAR(PRIX), CODE_COMMUNE,
      COALESCE(ID_PARCELLE, ''), TYPE_LOCAL, TO_VARCHAR(SURFACE_BATI),
      COALESCE(TO_VARCHAR(NB_PIECES), ''), COALESCE(LOT1_NUMERO, '')), 256) AS ID_VENTE,
    ID_MUTATION,
    DATE_VENTE,
    ANNEE,
    CODE_DEPARTEMENT,
    CODE_COMMUNE,
    NOM_COMMUNE,
    CODE_POSTAL,
    ADRESSE,
    ID_PARCELLE,
    TYPE_LOCAL,
    SURFACE_BATI,
    SURFACE_CARREZ,
    NB_PIECES,
    SURFACE_TERRAIN,
    NB_DEPENDANCES,
    PRIX,
    PRIX_M2,
    LONGITUDE,
    LATITUDE,
    (PRIX_M2 BETWEEN 300 AND 30000 AND SURFACE_BATI BETWEEN 9 AND 1000)     AS EST_PLAUSIBLE,
    (LONGITUDE IS NOT NULL AND LATITUDE IS NOT NULL)                        AS EST_GEOLOCALISE,
    ANNEE_FICHIER,
    SHA2(TO_JSON(OBJECT_CONSTRUCT(
      'id_mutation', ID_MUTATION, 'nom_commune', NOM_COMMUNE,
      'code_postal', CODE_POSTAL, 'adresse', ADRESSE,
      'surface_carrez', SURFACE_CARREZ, 'surface_terrain', SURFACE_TERRAIN,
      'nb_dependances', NB_DEPENDANCES, 'longitude', LONGITUDE,
      'latitude', LATITUDE, 'annee_fichier', ANNEE_FICHIER)), 256)          AS HASH_CONTENU
  FROM ventes
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ID_VENTE ORDER BY ID_MUTATION) = 1;

  SELECT COUNT(*) INTO :nb_source      FROM FONCIER.RAW.TMP_MUTATIONS_LOGEMENT;
  SELECT COUNT(*) INTO :nb_cible_avant FROM FONCIER.CORE.MUTATIONS_LOGEMENT;

  IF (NOT FORCER AND (nb_source = 0 OR nb_source < nb_cible_avant * 0.5)) THEN
    RAISE volume_anormal;
  END IF;

  MERGE INTO FONCIER.CORE.MUTATIONS_LOGEMENT t
  USING FONCIER.RAW.TMP_MUTATIONS_LOGEMENT s
    ON t.ID_VENTE = s.ID_VENTE
  WHEN MATCHED AND t.HASH_CONTENU <> s.HASH_CONTENU THEN UPDATE SET
    t.ID_MUTATION      = s.ID_MUTATION,
    t.NOM_COMMUNE      = s.NOM_COMMUNE,
    t.CODE_POSTAL      = s.CODE_POSTAL,
    t.ADRESSE          = s.ADRESSE,
    t.SURFACE_CARREZ   = s.SURFACE_CARREZ,
    t.SURFACE_TERRAIN  = s.SURFACE_TERRAIN,
    t.NB_DEPENDANCES   = s.NB_DEPENDANCES,
    t.LONGITUDE        = s.LONGITUDE,
    t.LATITUDE         = s.LATITUDE,
    t.EST_GEOLOCALISE  = s.EST_GEOLOCALISE,
    t.ANNEE_FICHIER    = s.ANNEE_FICHIER,
    t.HASH_CONTENU     = s.HASH_CONTENU,
    t.DATE_MAJ         = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  WHEN NOT MATCHED THEN INSERT (
    ID_VENTE, ID_MUTATION, DATE_VENTE, ANNEE, CODE_DEPARTEMENT, CODE_COMMUNE,
    NOM_COMMUNE, CODE_POSTAL, ADRESSE, ID_PARCELLE, TYPE_LOCAL, SURFACE_BATI,
    SURFACE_CARREZ, NB_PIECES, SURFACE_TERRAIN, NB_DEPENDANCES, PRIX, PRIX_M2,
    LONGITUDE, LATITUDE, EST_PLAUSIBLE, EST_GEOLOCALISE, ANNEE_FICHIER,
    HASH_CONTENU, DATE_MAJ)
  VALUES (
    s.ID_VENTE, s.ID_MUTATION, s.DATE_VENTE, s.ANNEE, s.CODE_DEPARTEMENT, s.CODE_COMMUNE,
    s.NOM_COMMUNE, s.CODE_POSTAL, s.ADRESSE, s.ID_PARCELLE, s.TYPE_LOCAL, s.SURFACE_BATI,
    s.SURFACE_CARREZ, s.NB_PIECES, s.SURFACE_TERRAIN, s.NB_DEPENDANCES, s.PRIX, s.PRIX_M2,
    s.LONGITUDE, s.LATITUDE, s.EST_PLAUSIBLE, s.EST_GEOLOCALISE, s.ANNEE_FICHIER,
    s.HASH_CONTENU, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);
  nb_merge := SQLROWCOUNT;

  DELETE FROM FONCIER.CORE.MUTATIONS_LOGEMENT t
  WHERE NOT EXISTS (
    SELECT 1 FROM FONCIER.RAW.TMP_MUTATIONS_LOGEMENT s
    WHERE s.ID_VENTE = t.ID_VENTE);
  nb_supprimes := SQLROWCOUNT;

  DROP TABLE IF EXISTS FONCIER.RAW.TMP_MUTATIONS_LOGEMENT;

  RETURN OBJECT_CONSTRUCT(
    'lignes_source',      nb_source,
    'lignes_cible_avant', nb_cible_avant,
    'lignes_mergees',     nb_merge,
    'lignes_supprimees',  nb_supprimes);
END;
$$;
