# Description

This is an ETF and FX rate collector geared towards Swiss investors.

It collects all ETFs traded on the Swiss stock exchange (SIX) every day
and collects monthly FX rates from the Swiss National Bank (SNB).

It writes the data into a `sqlite3` database.\
Use case: the script is intended to be run via a containerized solution
on a Linux server.

# Setup

## 1. Clone the repository

## 2. Install Docker or Podman

Make sure **either Docker or Podman** is installed on the system.

Only **one container runtime is required**.

## 3. Build the container image

### Docker

``` bash
chmod +x build_docker.sh
./build_docker.sh
```

### Podman

``` bash
chmod +x build_podman.sh
./build_podman.sh
```

## 4. Run the container manually (initial test)

Before setting up automation, run the container once manually to verify
everything works.

### Docker

``` bash
chmod +x run_docker.sh
./run_docker.sh
```

### Podman

``` bash
chmod +x run_podman.sh
./run_podman.sh
```

## 5. Set up a cron job

The script is designed to run periodically via `cron`.

Edit your crontab:

``` bash
crontab -e
```

### Example: Run every 12 hours using Docker

Adjust paths as needed.

``` bash
0 */12 * * * echo "$(date '+\%Y-\%m-\%d \%H:\%M:\%S') - Running script" >> /home/ubuntu/git-clones/github/user-repos/financedb/logs/cron.log 2>&1 && /home/ubuntu/git-clones/github/user-repos/financedb/run_docker.sh >> /home/ubuntu/git-clones/github/user-repos/financedb/logs/cron.log 2>&1
```

### Example: Run every 12 hours using Podman

``` bash
0 */12 * * * echo "$(date '+\%Y-\%m-\%d \%H:\%M:\%S') - Running script" >> /home/ubuntu/git-clones/github/user-repos/financedb/logs/cron.log 2>&1 && /home/ubuntu/git-clones/github/user-repos/financedb/run_podman.sh >> /home/ubuntu/git-clones/github/user-repos/financedb/logs/cron.log 2>&1
```

# Database Location

The SQLite database is stored locally in:

    db/market_data.db

The database directory is mounted into the container so that data
persists across container runs.

# Backup and Restore of the Database

Backups and restores are easily possible by sending/receiving files
to/from the server.

These examples assume that you have a `~/.ssh/config`, e.g.:

``` bash
Host financedb
    HostName IP
    User ubuntu
    Port PORT
    IdentityFile ~/.ssh/financedb
```

## Send a file from Linux to the Linux server

(Adjust path and server details)

``` bash
scp market_data.db financedb:/home/ubuntu/git-clones/github/user-repos/financedb/db/
```

## Receive a file on Linux from the Linux server

(Adjust path and server details)

``` bash
scp financedb:/home/ubuntu/git-clones/github/user-repos/financedb/db/market_data.db .
```
