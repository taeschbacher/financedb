#!/usr/bin/env python3
"""Create historical price charts for a fixed list of ISINs from market_data.db."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

try:
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
except ImportError as exc:
    raise SystemExit(
        "matplotlib is required for chart generation. Install it with:\n"
        "  python3 -m pip install matplotlib"
    ) from exc


ISINS = [
    "CH0237935652",
    "CH0237935637",
    "LU0147308422",
    "IE00B3XXRP09",
    "IE00B3VVMM84",
    "IE0005042456",
    "IE00B95PGT31",
    "IE000VAHT5T0",
]

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = REPO_ROOT / "db" / "market_data.db"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "charts"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate one historical closing-price chart per configured ISIN and "
            "an overview image containing all charts."
        )
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite database path (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for PNG charts (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Open the overview chart interactively after saving it.",
    )
    return parser.parse_args()


def validate_database(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'market_data'"
    ).fetchone()
    if row is None:
        raise RuntimeError("Table 'market_data' was not found in the SQLite database.")

    columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(market_data)").fetchall()
    }
    required = {
        "ISIN",
        "ShortName",
        "ValorSymbol",
        "ClosingPrice",
        "LatestTradeDate",
        "TradingBaseCurrency",
        "Exchange",
    }
    missing = required - columns
    if missing:
        raise RuntimeError(
            "Table 'market_data' is missing required columns: "
            + ", ".join(sorted(missing))
        )


def load_isin_data(
    conn: sqlite3.Connection, isin: str
) -> tuple[str, dict[str, list[tuple[datetime, float]]]]:
    rows = conn.execute(
        """
        SELECT
            ShortName,
            ValorSymbol,
            ClosingPrice,
            LatestTradeDate,
            TradingBaseCurrency,
            Exchange
        FROM market_data
        WHERE ISIN = ?
        ORDER BY LatestTradeDate, ValorSymbol, TradingBaseCurrency, Exchange
        """,
        (isin,),
    ).fetchall()

    if not rows:
        return isin, {}

    short_names: dict[str, int] = defaultdict(int)
    series: dict[str, list[tuple[datetime, float]]] = defaultdict(list)

    for short_name, symbol, price, date_text, currency, exchange in rows:
        try:
            trade_date = datetime.strptime(date_text, "%Y-%m-%d")
        except (TypeError, ValueError):
            print(
                f"Warning: skipping invalid LatestTradeDate {date_text!r} for {isin}",
                file=sys.stderr,
            )
            continue

        short_names[short_name] += 1
        label_parts = [symbol or "no-symbol", currency or "no-currency"]
        if exchange:
            label_parts.append(exchange)
        label = " / ".join(label_parts)
        series[label].append((trade_date, float(price)))

    display_name = max(short_names, key=short_names.get) if short_names else isin
    return display_name, dict(series)


def deduplicate_points(
    points: list[tuple[datetime, float]], isin: str, label: str
) -> tuple[list[datetime], list[float]]:
    by_date: dict[datetime, list[float]] = defaultdict(list)
    for trade_date, price in points:
        by_date[trade_date].append(price)

    dates: list[datetime] = []
    prices: list[float] = []
    for trade_date in sorted(by_date):
        values = by_date[trade_date]
        if len(values) > 1:
            print(
                f"Warning: {isin} ({label}) has {len(values)} prices on "
                f"{trade_date.date()}; plotting their average.",
                file=sys.stderr,
            )
        dates.append(trade_date)
        prices.append(sum(values) / len(values))
    return dates, prices


def format_axis(ax: plt.Axes) -> None:
    locator = mdates.AutoDateLocator(minticks=4, maxticks=10)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    ax.grid(True, alpha=0.25)
    ax.set_xlabel("Trade date")
    ax.set_ylabel("Closing price")


def plot_isin(
    ax: plt.Axes,
    isin: str,
    display_name: str,
    series: dict[str, list[tuple[datetime, float]]],
    *,
    compact: bool = False,
) -> None:
    if not series:
        ax.text(
            0.5,
            0.5,
            "No data found",
            ha="center",
            va="center",
            transform=ax.transAxes,
        )
        ax.set_title(isin)
        ax.set_axis_off()
        return

    for label, points in sorted(series.items()):
        dates, prices = deduplicate_points(points, isin, label)
        ax.plot(dates, prices, linewidth=1.4, label=label)

    title = f"{isin} — {display_name}"
    if compact and len(title) > 46:
        title = f"{isin} — {display_name[:28]}…"
    ax.set_title(title)
    format_axis(ax)

    if len(series) > 1:
        ax.legend(fontsize="small" if not compact else "x-small")
    else:
        only_label = next(iter(series))
        ax.text(
            0.01,
            0.98,
            only_label,
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize="small" if not compact else "x-small",
        )


def save_individual_chart(
    output_dir: Path,
    isin: str,
    display_name: str,
    series: dict[str, list[tuple[datetime, float]]],
) -> Path:
    fig, ax = plt.subplots(figsize=(12, 6), constrained_layout=True)
    plot_isin(ax, isin, display_name, series)
    fig.suptitle("Historical price development", fontsize=14)
    path = output_dir / f"{isin}.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path


def save_overview_chart(
    output_dir: Path,
    loaded: dict[str, tuple[str, dict[str, list[tuple[datetime, float]]]]],
) -> tuple[Path, plt.Figure]:
    fig, axes = plt.subplots(4, 2, figsize=(16, 18), constrained_layout=True)
    flat_axes = axes.ravel()

    for ax, isin in zip(flat_axes, ISINS):
        display_name, series = loaded[isin]
        plot_isin(ax, isin, display_name, series, compact=True)

    for ax in flat_axes[len(ISINS) :]:
        ax.set_axis_off()

    fig.suptitle("Historical price development — selected ISINs", fontsize=16)
    path = output_dir / "overview.png"
    fig.savefig(path, dpi=160)
    return path, fig


def main() -> int:
    args = parse_args()
    db_path = args.db.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()

    if not db_path.is_file():
        print(
            f"Database not found: {db_path}\n"
            "Copy market_data.db into the repository's db/ folder or pass --db.",
            file=sys.stderr,
        )
        return 2

    output_dir.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        validate_database(conn)
        loaded = {isin: load_isin_data(conn, isin) for isin in ISINS}

    print(f"Database: {db_path}")
    print(f"Charts:   {output_dir}")

    for isin in ISINS:
        display_name, series = loaded[isin]
        path = save_individual_chart(output_dir, isin, display_name, series)
        point_count = sum(len(points) for points in series.values())
        if point_count:
            print(f"  {isin}: {point_count} rows -> {path.name}")
        else:
            print(f"  {isin}: no rows found -> {path.name}")

    overview_path, overview_fig = save_overview_chart(output_dir, loaded)
    print(f"  Overview -> {overview_path.name}")

    if args.show:
        plt.show()
    else:
        plt.close(overview_fig)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
