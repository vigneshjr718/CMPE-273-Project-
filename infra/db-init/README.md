# infra/db-init: database roles and databases (owner: C)

Every `*.sql` file in this folder is run **in name order, on every `docker compose up`**, by the
`db-init` service (`infra/postgres/run-db-init.sh`). It runs as the `postgres` superuser against
the `postgres` database, before any application service starts. Because it runs on an existing
volume too, new roles and databases also reach the cloud VM.

## Rules

1. **Idempotent:** running a file twice in a row must succeed and change nothing the second time.
2. **No passwords in SQL.** For every role, add `<NAME>_DB_PASSWORD=...` to `infra/.env`
   (and `infra/.env.example` with a `local_only` value). db-init passes it to psql as
   `:'<name>_db_password'` (lower case). A missing variable makes db-init fail, and the apps
   then do not start.
3. Use hex passwords (`openssl rand -hex 16`). Compose puts them into connection URLs.
4. Only roles, databases and grants go here. Each service creates its own tables with its own
   migrations.
5. Keep `CREATE DATABASE` outside `BEGIN`/`COMMIT` and outside `DO` blocks: it cannot run
   inside a transaction. Role statements can (and in `init.sql` do) share one transaction.

`init.sql` (draft by D for C to review) applies this to all services with a small helper
function. Below is the same pattern for a single service, to show how it works.

## Example (tracker role + tracker_db)

The pattern for one role and its database.

```sql
-- Password comes from infra/.env TRACKER_DB_PASSWORD -> psql variable :'tracker_db_password'.
-- psql does not substitute :'vars' inside a DO $$...$$ body, so hand it over as a setting.
SELECT set_config('partstream.role_password', :'tracker_db_password', false);

-- Role: create it if missing; otherwise re-apply the password, so infra/.env stays the single
-- source of truth (rotating = edit infra/.env, then redeploy).
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'tracker') THEN
    EXECUTE format('CREATE ROLE tracker LOGIN PASSWORD %L',
                   current_setting('partstream.role_password'));
  ELSE
    EXECUTE format('ALTER ROLE tracker WITH LOGIN PASSWORD %L',
                   current_setting('partstream.role_password'));
  END IF;
END
$$;

-- Database: CREATE DATABASE cannot run inside DO/a transaction, so generate the statement
-- only when the database is missing and execute it with \gexec.
SELECT 'CREATE DATABASE tracker_db OWNER tracker'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'tracker_db')
\gexec
```
And in `infra/.env.example`: `TRACKER_DB_PASSWORD=local_only`.

## Test it twice

```bash
docker compose -f infra/docker-compose.yml run --rm db-init   # creates
docker compose -f infra/docker-compose.yml run --rm db-init   # must also exit 0
```
