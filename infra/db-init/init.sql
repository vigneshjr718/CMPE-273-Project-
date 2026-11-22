-- Owner: C — draft by D, please review.
--
-- Roles and databases for every PartStream service. Run by the db-init compose service
-- (infra/postgres/run-db-init.sh) as the postgres superuser on EVERY start, so this file is
-- idempotent: a second run succeeds and changes nothing.
--
-- Passwords are never written here. Each comes from infra/.env as <NAME>_DB_PASSWORD and
-- arrives as the psql variable :'<name>_db_password'. Re-running re-applies the password,
-- so infra/.env is the single source of truth (rotate = edit infra/.env, redeploy).
--
-- Each service role owns only its own database, and PUBLIC cannot connect to any of them.
-- Tables, schemas and grants inside a database belong to that service's own migrations.
--
-- REVIEW: "order" is a reserved SQL word, so the order service's role is order_svc.

-- ---------------------------------------------------------------------------------------
-- 1. Roles: all in one transaction, so a missing/empty password changes nothing at all.
-- ---------------------------------------------------------------------------------------
BEGIN;

-- Session-only helper (pg_temp): leaves nothing behind in the database.
CREATE FUNCTION pg_temp.ensure_login_role(role_name text, role_password text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF role_password IS NULL OR role_password = '' THEN
    RAISE EXCEPTION 'no password set for role % (add it to infra/.env)', role_name;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = role_name) THEN
    EXECUTE format('CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
                   role_name, role_password);
  ELSE
    EXECUTE format('ALTER ROLE %I WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
                   role_name, role_password);
  END IF;
END
$$;

SELECT pg_temp.ensure_login_role('order_svc',   :'order_db_password');
SELECT pg_temp.ensure_login_role('coordinator', :'coordinator_db_password');
SELECT pg_temp.ensure_login_role('tracker',     :'tracker_db_password');
SELECT pg_temp.ensure_login_role('seller1',     :'seller1_db_password');
SELECT pg_temp.ensure_login_role('seller2',     :'seller2_db_password');
SELECT pg_temp.ensure_login_role('seller3',     :'seller3_db_password');
SELECT pg_temp.ensure_login_role('seller4',     :'seller4_db_password');
SELECT pg_temp.ensure_login_role('seller5',     :'seller5_db_password');

COMMIT;

-- ---------------------------------------------------------------------------------------
-- 2. Databases: CREATE DATABASE cannot run in a transaction or DO block, so the statements
--    are generated only for missing databases and executed with \gexec.
-- ---------------------------------------------------------------------------------------
CREATE TEMP VIEW service_db (datname, owner) AS VALUES
  ('order_db',       'order_svc'),
  ('coordinator_db', 'coordinator'),
  ('tracker_db',     'tracker'),
  ('seller1_db',     'seller1'),
  ('seller2_db',     'seller2'),
  ('seller3_db',     'seller3'),
  ('seller4_db',     'seller4'),
  ('seller5_db',     'seller5');

SELECT format('CREATE DATABASE %I OWNER %I', s.datname, s.owner)
FROM service_db s
WHERE NOT EXISTS (SELECT FROM pg_database d WHERE d.datname = s.datname)
\gexec

-- ---------------------------------------------------------------------------------------
-- 3. Ownership and isolation, re-applied every run (also fixes databases created by hand).
--    Owners keep full rights on their own database; PUBLIC loses CONNECT/TEMPORARY.
-- ---------------------------------------------------------------------------------------
SELECT format('ALTER DATABASE %I OWNER TO %I', s.datname, s.owner),
       format('REVOKE ALL ON DATABASE %I FROM PUBLIC', s.datname)
FROM service_db s
\gexec
