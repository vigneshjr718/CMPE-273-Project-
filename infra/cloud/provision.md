# GCP provisioning record (Package D)

How the PartStream VM was built. It was provisioned **manually** (console + SSH). The `gcloud`
commands below are equivalents for rebuilding it, not a script that was run. Day-to-day
operation (deploy, backup, reboot checks, contacts) is in [docs/runbook.md](../../docs/runbook.md).

## Values for this deployment

**Never put secrets here.** The repository is public, so the external IP address itself is
written as `<STATIC_IP>`. Look it up with
`gcloud compute addresses describe partstream-ip --region us-west1 --format='value(address)'`.

| Item | Value |
|---|---|
| GCP project | `partstream` |
| VM name | `partstream-vm` |
| Zone / region | `us-west1-b` / `us-west1` |
| Machine type | `e2-standard-2` (2 vCPU, 8 GB) |
| Boot disk | 30 GB balanced persistent disk, Ubuntu 24.04 LTS x86/64 (`ubuntu-2404-lts-amd64`, project `ubuntu-os-cloud`) |
| Static external IP | `<STATIC_IP>` (address resource `partstream-ip`) |
| Firewall | `default-allow-http` (80) and `default-allow-https` (443) via network tags `http-server`, `https-server`. SSH (22) through the default rule |
| Data disk | `partstream-data`, 20 GB balanced persistent disk, created blank. Device `/dev/disk/by-id/google-partstream-data`. Deletion rule: **keep disk**. Default snapshot schedule enabled |
| LUKS | LUKS2, mapper `partstream_data` (`/dev/mapper/partstream_data`), keyfile `/etc/luks/partstream-data.key` |
| Mount point | `/mnt/data` (ext4) |
| Docker data-root | `/mnt/data/docker` |
| Repo checkout on VM | `~/CMPE-273-Project-` (note the trailing hyphen), from `https://github.com/vigneshjr718/CMPE-273-Project-.git` |
| Deployed branch | `feature/package-d-tracker` |
| Postgres superuser | `postgres` (password in `~/partstream-secrets.txt`) |
| Secrets | `~/partstream-secrets.txt` on the VM (mode 600), never in git |
| Monitoring | Google Cloud Ops Agent (logs + metrics) |

## 1. VM, static IP, firewall

```bash
gcloud config set project partstream
gcloud compute addresses create partstream-ip --region us-west1
gcloud compute disks create partstream-data --zone us-west1-b --size 20GB --type pd-balanced
gcloud compute instances create partstream-vm --zone us-west1-b \
  --machine-type e2-standard-2 \
  --image-family ubuntu-2404-lts-amd64 --image-project ubuntu-os-cloud \
  --boot-disk-size 30GB --boot-disk-type pd-balanced \
  --address partstream-ip --tags http-server,https-server \
  --disk name=partstream-data,device-name=partstream-data,mode=rw,auto-delete=no
# HTTP/HTTPS rules (the console checkboxes create these):
gcloud compute firewall-rules create default-allow-http  --allow tcp:80  --target-tags http-server
gcloud compute firewall-rules create default-allow-https --allow tcp:443 --target-tags https-server
```
- The data disk's deletion rule is **keep disk** (`auto-delete=no`), so deleting the VM does
  not delete the data.
- The console's default snapshot schedule is attached to the data disk. Those snapshots hold
  only LUKS ciphertext, and restoring one needs the keyfile (see §3).
- Only 80/443 (and SSH) are reachable from outside. Postgres (5432), Kafka (9092) and the
  service APIs are never published, because the cloud run omits `docker-compose.dev.yml`
  (see §6). Compose publishes only port 80. 443 is open for A's proxy if it terminates TLS.

## 2. Ops Agent

```bash
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
sudo bash add-google-cloud-ops-agent-repo.sh --also-install
sudo systemctl status google-cloud-ops-agent --no-pager
```

## 3. Encrypted data disk (LUKS2, auto-unlock at boot)

```bash
DEV=/dev/disk/by-id/google-partstream-data          # stable name; never use /dev/sdX
sudo install -d -m 700 /etc/luks
sudo dd if=/dev/urandom of=/etc/luks/partstream-data.key bs=512 count=8
sudo chmod 400 /etc/luks/partstream-data.key

sudo cryptsetup luksFormat --type luks2 "$DEV" /etc/luks/partstream-data.key
sudo cryptsetup open "$DEV" partstream_data --key-file /etc/luks/partstream-data.key
sudo mkfs.ext4 /dev/mapper/partstream_data
sudo mkdir -p /mnt/data

# Auto-unlock and mount at boot
echo "partstream_data $DEV /etc/luks/partstream-data.key luks" | sudo tee -a /etc/crypttab
echo "/dev/mapper/partstream_data /mnt/data ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab
sudo systemctl daemon-reload && sudo mount -a
```
`nofail` lets the VM still boot (and stay reachable over SSH) if the unlock fails. Docker then
refuses to start (§4), so nothing writes to the unencrypted boot disk by mistake.

### Security tradeoff (read this)
The keyfile lives on the **boot disk** so that the VM can unlock unattended after a reboot.
- **Protects against:** someone who gets the data disk **on its own**, e.g. a detached disk, a
  snapshot or image of only the data disk, or a disk reused after deletion. Without the key
  they get ciphertext.
- **Does NOT protect against:** anyone with root on the running VM, or anyone holding both the
  boot disk (or a boot-disk snapshot) and the data disk. In those cases the key is available.
  It also adds nothing beyond GCP's default at-rest encryption against Google-side access.
- **Availability risk:** if the boot disk is lost, the keyfile is lost, and the data disk cannot
  be decrypted. Keep an offline copy of the key (see runbook "LUKS key").
- **Snapshots:** the scheduled data-disk snapshots are encrypted by LUKS, so they are safe to
  hold. They are also useless without the key. If boot-disk snapshots exist too, whoever can read
  both can decrypt, so restrict snapshot access in IAM as you would the VM.
- **Alternatives** considered for the report: a passphrase typed at boot (stronger, but no
  unattended reboot); a key fetched from Secret Manager/Cloud KMS at boot (better separation,
  more setup); GCP CMEK on the disk (managed, but key custody stays inside GCP).

## 4. Docker on the encrypted disk

```bash
# Docker Engine + compose plugin (official apt repo), then:
sudo mkdir -p /mnt/data/docker
echo '{ "data-root": "/mnt/data/docker" }' | sudo tee /etc/docker/daemon.json

# Never start Docker unless /mnt/data is mounted
sudo mkdir -p /etc/systemd/system/docker.service.d
printf '[Unit]\nRequiresMountsFor=/mnt/data\n' | sudo tee /etc/systemd/system/docker.service.d/override.conf
sudo systemctl daemon-reload && sudo systemctl restart docker
sudo usermod -aG docker "$USER"    # log out and back in
```
All Docker images, containers and **named volumes** (`postgres-data`, `kafka-data`) are therefore
on the encrypted disk.

## 5. Secrets

```bash
umask 077
{
  echo "POSTGRES_PASSWORD=$(openssl rand -hex 16)"
  for v in ORDER COORDINATOR TRACKER SELLER1 SELLER2 SELLER3 SELLER4 SELLER5; do
    echo "${v}_DB_PASSWORD=$(openssl rand -hex 16)"
  done
} > ~/partstream-secrets.txt
chmod 600 ~/partstream-secrets.txt
```
- Every secret is `openssl rand -hex 16` (128 bits), one `NAME=value` line each.
  `POSTGRES_PASSWORD` is the password of the `postgres` superuser.
- `~/partstream-secrets.txt` is outside the repository and never committed. `infra/.env` is
  git-ignored. Add one line per new service role.
- The file is on the boot disk, so it is covered by the same tradeoff as the LUKS key.

## 6. Application config and first start

```bash
git clone https://github.com/vigneshjr718/CMPE-273-Project-.git ~/CMPE-273-Project-
cd ~/CMPE-273-Project-            # trailing hyphen is part of the name
git checkout feature/package-d-tracker
cp infra/.env.example infra/.env && chmod 600 infra/.env
# Replace every local_only / postgres_local_only value with the matching line from
# ~/partstream-secrets.txt (POSTGRES_PASSWORD and one <NAME>_DB_PASSWORD per service role).
```
Compose builds each service's `DATABASE_URL` from its `<NAME>_DB_PASSWORD`, so there are no URL
lines to edit. `deploy.sh` refuses to start if a password needed by `infra/db-init/*.sql` is
missing.
Start **without** `infra/docker-compose.dev.yml`. That file publishes Postgres/Kafka/API ports
and is for laptops only. `deploy.sh` deploys the branch that is checked out
(`feature/package-d-tracker` today). Name only the services whose code exists, because
placeholder services cannot be built yet:
```bash
infra/cloud/deploy.sh tracker
```
To confirm that only the frontend publishes a host port, run
`docker compose -f infra/docker-compose.yml --profile apps config | grep -A3 ports:`.
Postgres and Kafka are reached only on the internal `partstream` network.

## 7. Roles and databases (db-init)

Roles and databases come from `infra/db-init/*.sql` (owner C). The one-shot `db-init` service
runs them on **every** start, before any app, with the passwords from `infra/.env`. So they also
apply to this VM's existing volume. Details: runbook, "Database roles and databases (db-init)".
That section also has the one-time `infra/.env` migration for VMs set up before db-init existed.

**History:** before db-init existed, the tracker role and database on this VM were created by
hand, as below. `init.sql` keeps the same names, so it adopts them: it re-applies
`TRACKER_DB_PASSWORD` and the ownership, and leaves the data alone. Do not run this again.
```bash
cd ~/CMPE-273-Project-
PW=$(grep '^TRACKER_DB_PASSWORD=' ~/partstream-secrets.txt | cut -d= -f2)
docker compose -f infra/docker-compose.yml exec -T postgres \
  psql -U postgres -v ON_ERROR_STOP=1 -v pw="$PW" <<'SQL'
CREATE ROLE tracker LOGIN PASSWORD :'pw';
CREATE DATABASE tracker_db OWNER tracker;
SQL
```

## 8. Verify after any rebuild or reboot

```bash
lsblk -f                                    # data disk -> crypto_LUKS -> partstream_data ext4 /mnt/data
sudo cryptsetup status partstream_data      # type: LUKS2, active
findmnt /mnt/data
docker info --format '{{.DockerRootDir}}'   # /mnt/data/docker
systemctl show docker -p RequiresMountsFor  # includes /mnt/data
docker compose -f infra/docker-compose.yml --profile apps ps
curl -sI http://<STATIC_IP>/ | head -1      # from another machine
```
