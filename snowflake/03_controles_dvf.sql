-- =====================================================================
-- 03_controles_dvf.sql — Contrôles de fin d'étape 2
--
-- Étape 2 terminée quand :
--   - les volumes RAW par année sont identiques entre deux chargements ;
--   - une relance de la procédure renvoie lignes_mergees = 0 et
--     lignes_supprimees = 0 ;
--   - les prix médians au m² par département sont plausibles.
-- =====================================================================

USE ROLE R_IMMO_ENGINEER;
USE WAREHOUSE WH_IMMO_XS;

-- 1. RAW : volumes par fichier (à noter dans le README après chaque chargement)
SELECT
  ANNEE_FICHIER,
  COUNT(*)                     AS LIGNES,
  COUNT(DISTINCT ID_MUTATION)  AS MUTATIONS,
  MIN(DATE_MUTATION)           AS PREMIERE_DATE,
  MAX(DATE_MUTATION)           AS DERNIERE_DATE,
  MAX(DATE_CHARGEMENT)         AS CHARGE_LE,
  COUNT(DISTINCT ID_EXECUTION) AS NB_EXECUTIONS   -- doit valoir 1 : sinon le pre-copy script n'a pas purgé
FROM FONCIER.RAW.DVF_MUTATIONS
GROUP BY ANNEE_FICHIER
ORDER BY ANNEE_FICHIER;

-- 2. Idempotence de CORE : la seconde exécution doit renvoyer 0 / 0
CALL FONCIER.CORE.SP_BUILD_MUTATIONS_LOGEMENT();

-- 3. Entonnoir par département : mutations de vente contenant au moins
--    un logement, puis ventes à un seul logement retenues dans CORE
WITH candidates AS (
  SELECT r.CODE_DEPARTEMENT, COUNT(DISTINCT r.ID_MUTATION) AS MUTATIONS_AVEC_LOGEMENT
  FROM FONCIER.RAW.DVF_MUTATIONS r
  JOIN FONCIER.REF.PERIMETRE p ON p.CODE_DEPARTEMENT = r.CODE_DEPARTEMENT AND p.ACTIF
  WHERE r.NATURE_MUTATION = 'Vente'
    AND r.TYPE_LOCAL IN ('Maison', 'Appartement')
  GROUP BY r.CODE_DEPARTEMENT
),
retenues AS (
  SELECT CODE_DEPARTEMENT,
         COUNT(*)                   AS VENTES_CORE,
         COUNT_IF(EST_PLAUSIBLE)    AS PLAUSIBLES,
         COUNT_IF(EST_GEOLOCALISE)  AS GEOLOCALISEES
  FROM FONCIER.CORE.MUTATIONS_LOGEMENT
  GROUP BY CODE_DEPARTEMENT
)
SELECT
  c.CODE_DEPARTEMENT,
  c.MUTATIONS_AVEC_LOGEMENT,
  r.VENTES_CORE,
  ROUND(100 * r.VENTES_CORE   / c.MUTATIONS_AVEC_LOGEMENT, 1) AS PCT_RETENU,
  ROUND(100 * r.PLAUSIBLES    / r.VENTES_CORE, 1)             AS PCT_PLAUSIBLE,
  ROUND(100 * r.GEOLOCALISEES / r.VENTES_CORE, 1)             AS PCT_GEOLOCALISE
FROM candidates c
LEFT JOIN retenues r ON r.CODE_DEPARTEMENT = c.CODE_DEPARTEMENT
ORDER BY c.CODE_DEPARTEMENT;

-- 4. Prix au m² par département, type de bien et année (ventes plausibles)
SELECT
  CODE_DEPARTEMENT,
  TYPE_LOCAL,
  ANNEE,
  COUNT(*)                             AS VENTES,
  ROUND(MEDIAN(PRIX_M2))               AS PRIX_M2_MEDIAN,
  ROUND(APPROX_PERCENTILE(PRIX_M2, 0.1)) AS P10,
  ROUND(APPROX_PERCENTILE(PRIX_M2, 0.9)) AS P90
FROM FONCIER.CORE.MUTATIONS_LOGEMENT
WHERE EST_PLAUSIBLE
GROUP BY CODE_DEPARTEMENT, TYPE_LOCAL, ANNEE
ORDER BY CODE_DEPARTEMENT, TYPE_LOCAL, ANNEE;

-- 5. Intégrité : aucune ligne ne doit sortir
SELECT ID_VENTE, COUNT(*) FROM FONCIER.CORE.MUTATIONS_LOGEMENT GROUP BY ID_VENTE HAVING COUNT(*) > 1;

-- 6. Échantillon à vérifier à la main sur app.dvf.etalab.gouv.fr
SELECT DATE_VENTE, NOM_COMMUNE, ADRESSE, TYPE_LOCAL, SURFACE_BATI, NB_PIECES,
       NB_DEPENDANCES, PRIX, PRIX_M2, ID_PARCELLE
FROM FONCIER.CORE.MUTATIONS_LOGEMENT SAMPLE (10 ROWS);
