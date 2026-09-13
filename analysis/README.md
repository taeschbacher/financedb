# ISIN chart analysis

This directory is tracked by Git, including the Python analysis script and generated chart PNGs.
The local SQLite database remains outside version control via the existing `db/*.db` rule.

## Setup on Fedora

Python itself includes the `sqlite3` module. The chart script additionally needs `matplotlib`:

```bash
python3 -m venv .venv-analysis
source .venv-analysis/bin/activate
python3 -m pip install --upgrade pip matplotlib
```

## Run

From the repository root:

```bash
python3 analysis/analyze_isins.py
```

The script reads `db/market_data.db` by default and creates:

- one PNG per configured ISIN in `analysis/charts/`
- `analysis/charts/overview.png` containing all eight charts in one 4 x 2 figure

The configured ISINs are:

- `CH0237935652`
- `CH0237935637`
- `LU0147308422`
- `IE00B3XXRP09`
- `IE00B3VVMM84`
- `IE0005042456`
- `IE00B95PGT31`
- `IE000VAHT5T0`

To display the overview interactively as well:

```bash
python3 analysis/analyze_isins.py --show
```

Alternative database or output paths can be supplied with `--db` and `--output-dir`.
