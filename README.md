# Description

This is an ETF and FX rate collector geared towards Swiss investors.

It collects all ETFs traded on the Swiss stock exchange (SIX) every day and collects monthly FX rates from the Swiss National Bank (SNB).

It writes the data into a `sqlite3` database.  
Use case: the script is intended to be run on a Linux server.

# Setup

**1. Create a Python virtual environment called `venv`**

```bash
python -m venv venv
```

**2. Activate the virtual environment**

```bash
. venv/bin/activate
```

**3. Install the required Python packages into the virtual environment**

```bash
pip install -r requirements.txt
```

**4. Give executable rights to `run.sh`**

```bash
chmod +x run.sh
```

**5. Set up a cron job to execute `run.sh` every 12 hours and log executions**

Example (adjust the path as needed):

```bash
0 */12 * * * echo "$(date '+\%Y-\%m-\%d \%H:\%M:\%S') - Running script" >> /home/ubuntu/git-clones/github/user-repos/financedb/logs/cron.log 2>&1 && /home/ubuntu/git-clones/github/user-repos/financedb/run.sh >> /home/ubuntu/git-clones/github/user-repos/financedb/logs/cron.log 2>&1
```

# Backup and Restore of the Database

Backups and restores are easily possible by sending/receiving files to/from the server.

Example using Windows (with PuTTY) and Linux server.

**PuTTY File Transfer Guide:**  
[https://the.earth.li/~sgtatham/putty/0.60/htmldoc/Chapter5.html](https://the.earth.li/~sgtatham/putty/0.60/htmldoc/Chapter5.html)

## Send a file from Windows to the Linux server

(Adjust path and server details)

```bash
pscp -i .\financedb_private.ppk C:\PATH\financedb\market_data.db ubuntu@IP:/home/ubuntu/git-clones/github/user-repos/financedb/db/
```

## Receive a file on Windows from the Linux server

(Adjust path and server details)

```bash
pscp -i .\financedb_private.ppk ubuntu@IP:/home/ubuntu/git-clones/github/user-repos/financedb/db/market_data.db C:\PATH\financedb\
```
