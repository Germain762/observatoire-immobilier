-- =====================================================================
-- 00_setup.sql — Mise en place Snowflake du projet valeur verte
--
-- À exécuter une fois dans une worksheet Snowsight, avec ACCOUNTADMIN.
-- Relançable sans casse : tout est en IF NOT EXISTS.
--
-- Avant de lancer : remplacer les deux <CLE_PUBLIQUE_...> (section 5)
-- par le contenu des fichiers keys/rsa_*.pub, sans les lignes BEGIN/END.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------
-- 1. Garde-fou crédits
--    Le quota est en crédits, pas en dollars. Vérifier le prix du crédit
--    de l'édition dans Admin > Cost Management et ajuster si besoin.
--    Les notifications partent vers les administrateurs dont l'e-mail
--    est vérifié.
-- ---------------------------------------------------------------------
CREATE RESOURCE MONITOR IF NOT EXISTS RM_IMMO
  WITH CREDIT_QUOTA = 100
       FREQUENCY = MONTHLY
       START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 50 PERCENT DO NOTIFY
    ON 75 PERCENT DO NOTIFY
    ON 90 PERCENT DO SUSPEND
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- ---------------------------------------------------------------------
-- 2. Warehouses
--    Un warehouse dédié au Mirroring pour voir son coût à part.
--    Si les crédits deviennent justes, Microsoft recommande de réutiliser
--    un warehouse existant : pointer la connexion mirror sur WH_IMMO_XS.
-- ---------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS WH_IMMO_XS
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Chargements et transformations DVF';

CREATE WAREHOUSE IF NOT EXISTS WH_MIRROR_XS
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Réservé au Mirroring Fabric';

ALTER WAREHOUSE WH_IMMO_XS   SET RESOURCE_MONITOR = RM_IMMO;
ALTER WAREHOUSE WH_MIRROR_XS SET RESOURCE_MONITOR = RM_IMMO;

-- ---------------------------------------------------------------------
-- 3. Rôles
--    R_IMMO_ENGINEER : propriétaire de la base, utilisé par toi et par
--                      le pipeline de chargement.
--    R_IMMO_MIRROR   : lecture et streams sur CORE, pour le Mirroring.
-- ---------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS R_IMMO_ENGINEER COMMENT = 'Développement et chargement DVF';
CREATE ROLE IF NOT EXISTS R_IMMO_MIRROR   COMMENT = 'Mirroring Fabric (lecture CDC)';

GRANT ROLE R_IMMO_ENGINEER TO ROLE SYSADMIN;
GRANT ROLE R_IMMO_MIRROR   TO ROLE SYSADMIN;

SET MON_USER = CURRENT_USER();
GRANT ROLE R_IMMO_ENGINEER TO USER IDENTIFIER($MON_USER);

GRANT CREATE DATABASE ON ACCOUNT        TO ROLE R_IMMO_ENGINEER;
GRANT USAGE ON WAREHOUSE WH_IMMO_XS     TO ROLE R_IMMO_ENGINEER;
GRANT USAGE ON WAREHOUSE WH_MIRROR_XS   TO ROLE R_IMMO_MIRROR;

-- ---------------------------------------------------------------------
-- 4. Base et schémas, créés par le rôle propriétaire
-- ---------------------------------------------------------------------
USE ROLE R_IMMO_ENGINEER;
USE WAREHOUSE WH_IMMO_XS;

CREATE DATABASE IF NOT EXISTS FONCIER COMMENT = 'Ventes immobilières DVF';
CREATE SCHEMA IF NOT EXISTS FONCIER.RAW  COMMENT = 'DVF brut, tel que chargé';
CREATE SCHEMA IF NOT EXISTS FONCIER.CORE COMMENT = 'Mutations nettoyées, source du Mirroring';

-- Droits du Mirroring : CREATE STREAM, SELECT, SHOW et DESCRIBE sur les tables.
-- Les future grants demandent MANAGE GRANTS, d'où le retour en ACCOUNTADMIN.
USE ROLE ACCOUNTADMIN;

GRANT USAGE ON DATABASE FONCIER                      TO ROLE R_IMMO_MIRROR;
GRANT USAGE ON SCHEMA FONCIER.CORE                   TO ROLE R_IMMO_MIRROR;
GRANT CREATE STREAM ON SCHEMA FONCIER.CORE           TO ROLE R_IMMO_MIRROR;
GRANT SELECT ON ALL TABLES IN SCHEMA FONCIER.CORE    TO ROLE R_IMMO_MIRROR;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FONCIER.CORE TO ROLE R_IMMO_MIRROR;

-- Rappel pour l'étape 2 : R_IMMO_MIRROR ne possède pas les tables de CORE.
-- Créer MUTATIONS_LOGEMENT avec CHANGE_TRACKING = TRUE, sinon le stream
-- du Mirroring ne pourra pas être créé.

-- ---------------------------------------------------------------------
-- 5. Utilisateurs de service (authentification par paire de clés)
--    Fabric ne permet pas de choisir un rôle dans la connexion :
--    le DEFAULT_ROLE doit porter tous les droits nécessaires.
--
--    Repli si une connexion Fabric refuse la paire de clés : les comptes
--    trial acceptent encore le mot de passe seul, mais pas avec
--    TYPE = SERVICE. Créer alors l'utilisateur avec TYPE = PERSON et
--    un PASSWORD.
-- ---------------------------------------------------------------------
CREATE USER IF NOT EXISTS SVC_FABRIC_LOADER
  TYPE = SERVICE
  DEFAULT_ROLE = R_IMMO_ENGINEER
  DEFAULT_WAREHOUSE = WH_IMMO_XS
  DEFAULT_NAMESPACE = FONCIER.RAW
  RSA_PUBLIC_KEY = '<CLE_PUBLIQUE_LOADER>'
  COMMENT = 'Pipeline Fabric de chargement DVF';

CREATE USER IF NOT EXISTS SVC_FABRIC_MIRROR
  TYPE = SERVICE
  DEFAULT_ROLE = R_IMMO_MIRROR
  DEFAULT_WAREHOUSE = WH_MIRROR_XS
  DEFAULT_NAMESPACE = FONCIER.CORE
  RSA_PUBLIC_KEY = '<CLE_PUBLIQUE_MIRROR>'
  COMMENT = 'Mirroring Fabric';

GRANT ROLE R_IMMO_ENGINEER TO USER SVC_FABRIC_LOADER;
GRANT ROLE R_IMMO_MIRROR   TO USER SVC_FABRIC_MIRROR;

-- ---------------------------------------------------------------------
-- 6. Vérifications
-- ---------------------------------------------------------------------
SHOW WAREHOUSES LIKE 'WH_%';
SHOW RESOURCE MONITORS LIKE 'RM_IMMO';
SHOW GRANTS TO ROLE R_IMMO_MIRROR;
DESC USER SVC_FABRIC_LOADER;   -- RSA_PUBLIC_KEY_FP doit être renseigné
DESC USER SVC_FABRIC_MIRROR;

-- Serveur à saisir dans les connexions Fabric
SELECT LOWER(CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME())
       || '.snowflakecomputing.com' AS SERVEUR_FABRIC;
