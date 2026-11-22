# PartStream operations runbook (Package D)

For whoever keeps the cloud deployment running during the presentation (Dec 7) and the grading
window afterwards. How the VM was built: [infra/cloud/provision.md](../infra/cloud/provision.md).
The public IP is written as `<STATIC_IP>` because the repository is public. Get it with
`gcloud compute addresses describe partstream-ip --region us-west1 --format='value(address)'`.

## Access and contacts

| | |
|---|---|
| Public URL | `http://<STATIC_IP>/` (static address `partstream-ip`) |
| VM | `partstream-vm`, `us-west1-b`, project `partstream`, e2-standard-2, Ubuntu 24.04 LTS |
| SSH | `gcloud compute ssh partstream-vm --zone us-west1-b --project partstream` |
| Repo on VM | `~/CMPE-273-Project-` (trailing hyphen) |
| Deployed branch | `feature/package-d-tracker` |
| Secrets | `~/partstream-secrets.txt` on the VM (mode 600), never in git. Postgres superuser: `postgres` |
| Grading access must last until | **TBD: confirm with instructor** |
| Primary contact after Dec 7 | **TBD** |
| Backup contact after Dec 7 | **TBD** |

The people listed need SSH access (project IAM) and must know where `~/partstream-secrets.txt`
lives. Never paste its contents into chat, issues or git.

## What runs where

- One VM runs everything with Docker Compose (`infra/docker-compose.yml` only, **no** dev
  override).
- **Published:** only the frontend/proxy on port 80. The firewall also allows 443 (for TLS if
  A's proxy adds it) and SSH.
- **Internal only:** Postgres 5432 and Kafka 9092 on the `partstream` network. Services: order
  8001, coordinator 8002, tracker 8004, sellers 8101-8105.
- **Persistent state:** Docker named volumes `postgres-data` and `kafka-data` under
  `/mnt/data/docker`, on the LUKS2-encrypted data disk `partstream-data` (20 GB balanced,
  deletion rule "keep disk", default snapshot schedule).
- **Disk snapshots** are a second, block-level backup. They are crash-consistent copies of the
  LUKS ciphertext. Restoring one means creating a disk from it, attaching it, and unlocking it
  with the same keyfile. They complement `scripts/backup.sh`; they don't replace it.
- **Single points of failure (state these honestly in the report):** one VM, one Kafka broker,
  one Postgres. Durable disks are not backups.

## Everyday commands (on the VM, from `~/CMPE-273-Project-`)

```bash
C="docker compose -f infra/docker-compose.yml --profile apps"
$C ps                                   # status and health
$C logs -f --tail=100 tracker           # JSON logs (also shipped by the Ops Agent)
infra/cloud/deploy.sh tracker           # pull the checked-out branch, rebuild, wait until healthy
infra/cloud/deploy.sh                   # all app services (once every owner's service builds)
DEPLOY_BRANCH=main infra/cloud/deploy.sh tracker   # switch branches (e.g. after the PR merges)
```
Internal health checks (the ports are not published, so run them inside the network):
```bash
$C exec tracker python -c "import urllib.request as u; print(u.urlopen('http://localhost:8004/ready').read().decode())"
curl -s http://<STATIC_IP>/api/status/orders/<order_id>     # public path, once A's proxy is deployed
```
`/ready` → `checks.dead_letter_events` > 0 means some producer sent a message the tracker could
not parse. The tracker keeps running. Inspect the messages with:
```bash
$C exec -T postgres psql -U postgres -d tracker_db -c \
  "SELECT id, topic, kafka_partition, kafka_offset, reason, event_id, received_at FROM dead_letter_events ORDER BY id DESC LIMIT 20"
```

## Reboot test (do before Dec 7, and after any VM change)

```bash
sudo reboot
# reconnect, then:
findmnt /mnt/data && sudo cryptsetup status partstream_data | head -2   # unlocked + mounted
docker info --format '{{.DockerRootDir}}'                         # /mnt/data/docker
docker compose -f infra/docker-compose.yml --profile apps ps     # all Up / healthy
```
Containers come back by themselves (`restart: unless-stopped`). Then, from another computer with
all team laptops off, open `http://<STATIC_IP>/` and look up an existing order. Each owner then
checks their own data (orders, workflows, inventory, tracker status).

**If `/mnt/data` is not mounted after the reboot:** Docker will not start (by design). Check
`journalctl -b -u systemd-cryptsetup@partstream_data` and that `/etc/luks/partstream-data.key` exists,
then run `sudo systemctl start systemd-cryptsetup@partstream_data && sudo mount /mnt/data &&
sudo systemctl start docker`.

## Backups

```bash
scripts/backup.sh                 # -> /mnt/data/backups/<UTC timestamp>/
KEEP_BACKUPS=14 scripts/backup.sh # same, then keep only the newest 14
```
- Contents: `pg_dump` (custom format) per database, `globals.sql` (roles and password hashes)
  and `SHA256SUMS`. Files are mode 600.
- Each database is consistent on its own. Databases are dumped one after another, not as one
  global snapshot.
- **Kafka is not backed up.** Topics keep events forever on the `kafka-data` volume. The
  Postgres databases of the producers (order, coordinator) are the system of record.
- `/mnt/data/backups` is on the **same disk** as the data. Before the presentation, and after
  important demos, copy the newest backup off the VM (e.g. `gcloud storage cp -r` to a
  private bucket, or `gcloud compute scp` to a laptop). Treat the copy as sensitive. It holds
  role password hashes.
- Suggested schedule: one backup before each demo, plus a daily cron during the grading window:
  `0 9 * * * cd ~/CMPE-273-Project- && KEEP_BACKUPS=14 scripts/backup.sh >> ~/backup.log 2>&1`

## Restore (rehearse once before Dec 7)

```bash
docker compose -f infra/docker-compose.yml --profile apps stop        # infra keeps running
scripts/restore.sh --yes --replay-tracker /mnt/data/backups/<stamp>              # everything
scripts/restore.sh --yes --replay-tracker /mnt/data/backups/<stamp> tracker_db   # one DB
docker compose -f infra/docker-compose.yml --profile apps up -d --wait
```
- Each restored database is dropped and recreated from the dump. Restoring `globals.sql` sets
  role passwords back to their values at backup time. The next `up` (db-init) re-applies the
  passwords from `infra/.env`.
- **Kafka offsets:** a restored database is older than the consumer offsets saved in Kafka.
  - `--replay-tracker` rewinds the tracker's consumer group to the earliest offset. The tracker
    then re-reads every event. Events it already has are skipped by `event_id`, and the missing
    ones are applied again.
  - Other consumers (e.g. the coordinator's `coordinator-v1` group) have the same issue.
    Their owners decide how to rewind them, so ask before restoring their databases.
- Tested locally: restoring an older `tracker_db` with `--replay-tracker` brought back an order
  published after the backup, with no duplicate history rows or dead-letter rows.
- After a restore, each owner verifies their own data.

## LUKS key

- The keyfile `/etc/luks/partstream-data.key` (root, mode 400) is on the boot disk, so the VM
  can reboot unattended.
- **Make an offline copy now.** If the boot disk is lost, the data disk cannot be decrypted:
  ```bash
  sudo cat /etc/luks/partstream-data.key | base64 > partstream-data.key.b64   # copy off-VM, then delete here
  ```
  Store it in a password manager or other private place, not in git or the team chat.
- Add a second key slot (e.g. a long passphrase) as a break-glass option:
  `sudo cryptsetup luksAddKey /dev/disk/by-id/google-partstream-data --key-file /etc/luks/partstream-data.key`
- What it protects against, and what it doesn't: see provision.md §3, "Security tradeoff".

## Database roles and databases (db-init)

- On **every** `up`, the one-shot `db-init` service runs `infra/db-init/*.sql` (owner C) as
  the `postgres` superuser, before any app starts. Apps wait for it to succeed. This is why
  new roles and databases also reach the VM's existing volume. Postgres's own "first start
  only" init folder is not used.
- The SQL is idempotent: it creates what is missing and re-applies passwords and ownership.
  Each database is owned by its service role only, and PUBLIC cannot connect.
- Passwords: each `<NAME>_DB_PASSWORD` in `infra/.env` becomes the psql variable
  `<name>_db_password`. Compose also builds each service's `DATABASE_URL` from the same value.
- Run it by hand (safe to repeat):
  ```bash
  docker compose -f infra/docker-compose.yml run --rm db-init
  ```
- If it fails, no app starts. `docker compose -f infra/docker-compose.yml logs db-init` names the
  file and error (passwords are never printed). The usual cause is a `*_DB_PASSWORD` missing from
  `infra/.env`. `deploy.sh` checks for that before starting.

### One-time migration of the VM's infra/.env

Older `infra/.env` files held `TRACKER_DATABASE_URL` / `COORDINATOR_DATABASE_URL`. Now one
password per role is needed. On the VM:
```bash
cd ~/CMPE-273-Project-
# 1. Make sure ~/partstream-secrets.txt has every role. Add the missing ones without
#    touching existing values (TRACKER_DB_PASSWORD must stay what the tracker role uses today):
for v in ORDER COORDINATOR TRACKER SELLER1 SELLER2 SELLER3 SELLER4 SELLER5; do
  grep -q "^${v}_DB_PASSWORD=" ~/partstream-secrets.txt \
    || echo "${v}_DB_PASSWORD=$(openssl rand -hex 16)" >> ~/partstream-secrets.txt
done
# 2. Copy those lines into infra/.env and drop the old URL lines:
sed -i '/_DATABASE_URL=/d; /_DB_PASSWORD=/d' infra/.env
grep '_DB_PASSWORD=' ~/partstream-secrets.txt >> infra/.env
infra/cloud/deploy.sh tracker
```

## Secrets

- Created by `openssl rand -hex 16` into `~/partstream-secrets.txt` (mode 600, outside git)
  and copied into `infra/.env` (mode 600, git-ignored). `deploy.sh` refuses to run while
  `infra/.env` still contains the `local_only` placeholders or lacks a password the SQL needs.
- **Rotate a service password:** generate a new value, put it in the secrets file and
  `infra/.env`, then run `infra/cloud/deploy.sh <service>`. db-init applies it to the role, and
  the service gets the new URL. Tested locally: the old password stops working.
- `POSTGRES_PASSWORD` in `infra/.env` only takes effect when the volume is first created.
  Changing it later needs `ALTER ROLE postgres` as well, because db-init logs in with it.

## Troubleshooting

| Symptom | Check |
|---|---|
| Site down from outside | `$C ps`; is the frontend running; firewall tags `http-server`; static IP still attached |
| No app starts after deploy | `$C logs db-init kafka-init`: missing `*_DB_PASSWORD`, SQL error, or topic problem |
| Tracker not ready | `/ready` checks: `database_and_migrations` failed → Postgres/credentials; consumer `restarting` → `$C logs kafka tracker` |
| Kafka unhealthy | `$C logs --tail=200 kafka`; disk space; never delete `kafka-data` to "fix" it |
| Disk full | `df -h /mnt/data`; `docker system df`; prune old images (`docker image prune`), old backups (`KEEP_BACKUPS`) |
| Status shows "processing" for a long time | Coordinator is behind or stopped (`$C logs coordinator`); the tracker only shows what B has published |

## Shutting down after grading

Only after the grading window is confirmed over: take a final backup, copy it off-VM, then stop
or delete the VM. The data disk has `auto-delete=no`, so delete it separately if it is no longer
needed. Release the static IP (it is billed while reserved).
