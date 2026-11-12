-- =====================================================================
-- 04_test_mirroring.sql — Vérifications de l'étape 3 côté Snowflake
--
-- À dérouler bloc par bloc, en alternant avec Fabric (docs/etape-3-mirroring.md).
-- La procédure de CORE sert de « réparateur » : elle remet la table dans
-- l'état calculé depuis RAW, ce qui teste au passage l'UPDATE et l'INSERT.
-- =====================================================================

USE ROLE R_IMMO_ENGINEER;
USE WAREHOUSE WH_IMMO_XS;

-- ---------------------------------------------------------------------
-- A. Avant de créer le miroir
-- ---------------------------------------------------------------------

-- La colonne change_tracking doit valoir ON
SHOW TABLES LIKE 'MUTATIONS_LOGEMENT' IN SCHEMA FONCIER.CORE;

-- Référence à comparer avec Fabric (Monitor replication et SQL analytics endpoint)
SELECT COUNT(*) AS LIGNES, MAX(DATE_MAJ) AS DERNIERE_MAJ
FROM FONCIER.CORE.MUTATIONS_LOGEMENT;

SELECT CODE_DEPARTEMENT, ANNEE, COUNT(*) AS VENTES
FROM FONCIER.CORE.MUTATIONS_LOGEMENT
GROUP BY CODE_DEPARTEMENT, ANNEE
ORDER BY CODE_DEPARTEMENT, ANNEE;

-- ---------------------------------------------------------------------
-- B. Après la copie initiale : le stream créé par le Mirroring
--    Il appartient à R_IMMO_MIRROR ; le voir demande ACCOUNTADMIN.
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
SHOW STREAMS IN SCHEMA FONCIER.CORE;
USE ROLE R_IMMO_ENGINEER;

-- ---------------------------------------------------------------------
-- C. Test de propagation : une ligne modifiée, une ligne supprimée
--    Les deux lignes de test sont choisies de façon déterministe.
-- ---------------------------------------------------------------------
SET ID_MODIF = (SELECT MIN(ID_VENTE) FROM FONCIER.CORE.MUTATIONS_LOGEMENT);
SET ID_SUPPR = (SELECT MAX(ID_VENTE) FROM FONCIER.CORE.MUTATIONS_LOGEMENT);
SELECT $ID_MODIF AS ID_MODIF, $ID_SUPPR AS ID_SUPPR;   -- noter les deux valeurs

-- Modification : marqueur dans l'adresse ; l'empreinte faussée garantit
-- que la procédure remettra la ligne d'origine à l'étape D.
UPDATE FONCIER.CORE.MUTATIONS_LOGEMENT
SET ADRESSE      = COALESCE(ADRESSE, '') || ' [TEST MIRRORING]',
    HASH_CONTENU = 'TEST_MIRRORING',
    DATE_MAJ     = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
WHERE ID_VENTE = $ID_MODIF;

DELETE FROM FONCIER.CORE.MUTATIONS_LOGEMENT
WHERE ID_VENTE = $ID_SUPPR;

-- => Dans Fabric : la ligne ID_MODIF porte le marqueur, ID_SUPPR a disparu,
--    le total a baissé de 1. Si la table n'avait pas changé depuis
--    longtemps, la détection peut prendre jusqu'à une heure.

-- ---------------------------------------------------------------------
-- D. Remise en état par la procédure (UPDATE + INSERT propagés)
--    Résultat attendu : lignes_mergees = 2, lignes_supprimees = 0
-- ---------------------------------------------------------------------
CALL FONCIER.CORE.SP_BUILD_MUTATIONS_LOGEMENT();

-- => Dans Fabric : marqueur disparu, ID_SUPPR revenu, total d'origine.

-- ---------------------------------------------------------------------
-- E. Coût du Mirroring (ACCOUNT_USAGE : quelques heures de latence)
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

SELECT WAREHOUSE_NAME,
       DATE_TRUNC('day', START_TIME)  AS JOUR,
       ROUND(SUM(CREDITS_USED), 3)    AS CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME, JOUR
ORDER BY JOUR, WAREHOUSE_NAME;

-- Requêtes émises par l'utilisateur du Mirroring
SELECT DATE_TRUNC('hour', START_TIME) AS HEURE,
       WAREHOUSE_NAME,
       COUNT(*)                       AS REQUETES,
       ROUND(SUM(TOTAL_ELAPSED_TIME) / 1000, 1) AS SECONDES
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE USER_NAME = 'SVC_FABRIC_MIRROR'
  AND START_TIME >= DATEADD(day, -2, CURRENT_TIMESTAMP())
GROUP BY HEURE, WAREHOUSE_NAME
ORDER BY HEURE;
