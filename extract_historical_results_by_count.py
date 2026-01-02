#!/usr/bin/env python3
"""Extract clean tables from the CBC website export .xls.

Input file in this folder:
    HistoricalResultsByCount [CAPC-1972-2025].xls

This spreadsheet is a web export with lots of header text and a repeating 3-row
block per species (Number / Num/Party Hrs / Flags). This script extracts:

- Species counts (Number rows) -> a clean CSV: Species, 1971, 1972, ...
- Participants -> a clean year table
- Effort (hours) -> a clean year table
- Weather -> a clean year table

It also removes artefacts:
- Drops blank species rows
- Stops at (and includes) the last expected species row "House Sparrow" if present
- Converts non-numeric year cells (e.g., 'cw') to 0

Usage (from repo root):
    python source/cbc_website/extract_historical_results_by_count.py \
        --input "source/cbc_website/HistoricalResultsByCount [CAPC-1972-2025].xls"

Outputs (defaults; can be overridden with args):
- CAPC species-by-year counts: source/cbc_website/CAPC/CAPC_<firstYear>_<lastYear>.csv
- Participants: source/cbc_website/CAPC/CAPC_participants.csv
- Effort: source/cbc_website/CAPC/CAPC_effort.csv
- Weather: source/cbc_website/CAPC/CAPC_weather.csv

Requires: pandas, xlrd
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


YEAR_RE = re.compile(r"\s*(19\d{2}|20\d{2})\b")


def _find_header(df: pd.DataFrame) -> tuple[int, int]:
    """Return (header_row_index, species_col_index)."""
    for i in range(len(df)):
        row = df.iloc[i].astype(str).str.strip().tolist()
        for j, cell in enumerate(row):
            if cell == "Species":
                return i, j
    raise ValueError("Could not find a header cell exactly equal to 'Species'")


def _find_row_with_cell(df: pd.DataFrame, *, exact: str) -> int:
    target = exact.strip()
    for i in range(len(df)):
        row = df.iloc[i].astype(str).str.strip().tolist()
        if target in row:
            return i
    raise ValueError(f"Could not find a row containing cell {exact!r}")


def _year_columns(header_row: pd.Series) -> list[tuple[int, int]]:
    cols: list[tuple[int, int]] = []
    for j, cell in enumerate(header_row.tolist()):
        m = YEAR_RE.match(str(cell))
        if m:
            cols.append((j, int(m.group(1))))
    if not cols:
        raise ValueError("Could not find any year columns in the header row")
    # De-dup by year, keep first occurrence
    seen: set[int] = set()
    out: list[tuple[int, int]] = []
    for j, y in cols:
        if y not in seen:
            out.append((j, y))
            seen.add(y)
    return out


def extract_year_header_metadata(
    input_path: Path,
    *,
    sheet_name: str | int = 0,
) -> pd.DataFrame:
    """Parse the species-year header cells to get Year/CountIndex/CountDate metadata.

    The sheet stores a rich header cell per year like:
      2024 [125]\nCount Date: 12/15/2024\n# Participants: ...
    """
    df = pd.read_excel(input_path, sheet_name=sheet_name, header=None, engine="xlrd")
    header_row, _ = _find_header(df)
    header_cells = df.iloc[header_row].astype(str).tolist()

    rows: list[dict[str, object]] = []
    for cell in header_cells:
        cell = str(cell)
        m = re.match(r"\s*(19\d{2}|20\d{2})\s*\[(\d+)\]", cell)
        if not m:
            continue
        year = int(m.group(1))
        count_index = int(m.group(2))

        count_date = None
        m_date = re.search(r"Count Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", cell)
        if m_date:
            count_date = m_date.group(1)

        rows.append({"CountIndex": count_index, "Year": year, "CountDate": count_date})

    out = pd.DataFrame(rows).drop_duplicates(subset=["CountIndex"], keep="first")
    return out


def _parse_count(v) -> int:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 0
    if isinstance(v, (int,)):
        return int(v)
    if isinstance(v, float):
        # counts should be whole numbers
        return int(round(v))
    s = str(v).strip()
    if s == "":
        return 0
    if s.lower() == "cw":
        # count-week marker in this export; treat as 0 for count-day totals
        return 0
    # plain integer or integer-like float
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    if re.fullmatch(r"-?\d+\.0+", s):
        return int(float(s))
    # Anything else is an artefact; drop to 0.
    return 0


def _parse_temp_f(v) -> float | None:
    """Parse a temperature cell like '42.0 Fahrenheit' or '10.6 Celsius' to Fahrenheit."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if not s:
        return None
    m = re.match(r"\s*(-?\d+(?:\.\d+)?)\s*(Celsius|Fahrenheit)\s*$", s, flags=re.IGNORECASE)
    if not m:
        # Sometimes these cells are just a number; treat it as unknown-unit and drop.
        try:
            float(s)
        except ValueError:
            return None
        return None
    val = float(m.group(1))
    unit = m.group(2).lower()
    if unit.startswith("f"):
        return val
    # C -> F
    return val * 9.0 / 5.0 + 32.0


def _clean_text(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v)
    # Normalize newlines and repeated whitespace
    s = re.sub(r"\s+", " ", s.replace("\n", " ")).strip()
    return s


def extract_counts(
    input_path: Path,
    output_path: Path | None,
    *,
    sheet_name: str = 0,
    stop_species: str = "House Sparrow",
) -> pd.DataFrame:
    df = pd.read_excel(input_path, sheet_name=sheet_name, header=None, engine="xlrd")
    header_row, species_col = _find_header(df)
    years = _year_columns(df.iloc[header_row])

    # Keep only the "Number" rows: those where the species column is non-empty.
    work = df.loc[header_row + 1 :, [species_col] + [j for j, _ in years]].copy()
    work = work[work[species_col].notna()]

    # Species common name is first line (before the scientific name in brackets).
    species_common = work[species_col].astype(str).str.split("\n", n=1).str[0].str.strip()
    work.insert(0, "Species", species_common)

    # Build output frame.
    out = pd.DataFrame({"Species": work["Species"]})
    for col_idx, year in years:
        out[str(year)] = work[col_idx].map(_parse_count)

    # Guarantee no blanks/NA in numeric fields.
    year_cols = [c for c in out.columns if c != "Species"]
    out[year_cols] = out[year_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)

    # Drop artefact rows (blank species)
    out = out[out["Species"].notna() & (out["Species"].astype(str).str.strip() != "")]

    # Stop at House Sparrow if present.
    stop_idx = out.index[out["Species"].eq(stop_species)]
    if len(stop_idx) > 0:
        out = out.loc[: stop_idx[0]]

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(output_path, index=False)
    return out


def extract_participants_effort(
    input_path: Path,
    *,
    sheet_name: str | int = 0,
) -> pd.DataFrame:
    """Extract the year-by-year table that includes Count Date, Participants, Hours, Species Reported."""
    df = pd.read_excel(input_path, sheet_name=sheet_name, header=None, engine="xlrd")

    # This table's header row contains these labels.
    header_row = _find_row_with_cell(df, exact="Count Date")
    header = df.iloc[header_row].astype(str).str.strip().tolist()

    def col_idx(label: str) -> int:
        try:
            return header.index(label)
        except ValueError as e:
            raise ValueError(f"Missing expected column label {label!r} in participants/effort header") from e

    idx_year = col_idx("Year")
    idx_count_date = col_idx("Count Date")
    idx_participants = col_idx("Num. Participants")
    idx_hours = col_idx("Num. Hours")
    idx_species_reported = col_idx("Num. Species Reported")

    # Read downward until Year is empty.
    rows = []
    for r in range(header_row + 1, len(df)):
        year_index = df.iat[r, idx_year]
        if year_index is None or (isinstance(year_index, float) and pd.isna(year_index)):
            break
        # year_index values are count indices (e.g., 124), not the actual year.
        count_date = df.iat[r, idx_count_date]
        rows.append(
            {
                "CountIndex": int(year_index) if str(year_index).strip() != "" else None,
                "CountDate": _clean_text(count_date),
                "Year": int(str(count_date).strip()[-4:]) if _clean_text(count_date) else None,
                "NumParticipants": int(df.iat[r, idx_participants]) if _clean_text(df.iat[r, idx_participants]) else None,
                "NumHours": float(df.iat[r, idx_hours]) if _clean_text(df.iat[r, idx_hours]) else None,
                "NumSpeciesReported": int(df.iat[r, idx_species_reported]) if _clean_text(df.iat[r, idx_species_reported]) else None,
            }
        )

    out = pd.DataFrame(rows)
    # Drop any accidental blank rows
    out = out[out["CountIndex"].notna()]
    return out


def extract_weather(
    input_path: Path,
    *,
    sheet_name: str | int = 0,
) -> pd.DataFrame:
    """Extract the Weather section (year-indexed) into a clean table."""
    df = pd.read_excel(input_path, sheet_name=sheet_name, header=None, engine="xlrd")

    # Weather header row includes these labels.
    header_row = _find_row_with_cell(df, exact="Low Temp.")
    header = df.iloc[header_row].astype(str).str.strip().tolist()

    def maybe_idx(label: str) -> int | None:
        try:
            return header.index(label)
        except ValueError:
            return None

    idx_year = maybe_idx("Year")
    if idx_year is None:
        raise ValueError("Weather header row is missing 'Year' column")

    idx_low = maybe_idx("Low Temp.")
    idx_high = maybe_idx("High Temp.")
    idx_am_clouds = maybe_idx("AM Clouds")
    idx_pm_clouds = maybe_idx("PM Clouds")
    idx_am_rain = maybe_idx("AM Rain")
    idx_pm_rain = maybe_idx("PM Rain")
    idx_am_snow = maybe_idx("AM Snow")
    idx_pm_snow = maybe_idx("PM Snow")

    rows = []
    for r in range(header_row + 1, len(df)):
        year_index = df.iat[r, idx_year]
        if year_index is None or (isinstance(year_index, float) and pd.isna(year_index)):
            break
        year_index = int(year_index)
        rows.append(
            {
                "CountIndex": year_index,
                "LowTempF": _parse_temp_f(df.iat[r, idx_low]) if idx_low is not None else None,
                "HighTempF": _parse_temp_f(df.iat[r, idx_high]) if idx_high is not None else None,
                "AMClouds": _clean_text(df.iat[r, idx_am_clouds]) if idx_am_clouds is not None else "",
                "PMClouds": _clean_text(df.iat[r, idx_pm_clouds]) if idx_pm_clouds is not None else "",
                "AMRain": _clean_text(df.iat[r, idx_am_rain]) if idx_am_rain is not None else "",
                "PMRain": _clean_text(df.iat[r, idx_pm_rain]) if idx_pm_rain is not None else "",
                "AMSnow": _clean_text(df.iat[r, idx_am_snow]) if idx_am_snow is not None else "",
                "PMSnow": _clean_text(df.iat[r, idx_pm_snow]) if idx_pm_snow is not None else "",
            }
        )

    out = pd.DataFrame(rows)
    out = out[out["CountIndex"].notna()]
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("source/cbc_website/CAPC/HistoricalResultsByCount [CAPC-1972-2025].xls"),
    )
    parser.add_argument(
        "--counts-output",
        type=Path,
        default=None,
        help="Species-by-year counts output CSV",
    )
    parser.add_argument(
        "--participants-output",
        type=Path,
        default=Path("source/cbc_website/CAPC/CAPC_participants.csv"),
        help="Participants output CSV",
    )
    parser.add_argument(
        "--effort-output",
        type=Path,
        default=Path("source/cbc_website/CAPC/CAPC_effort.csv"),
        help="Effort output CSV",
    )
    parser.add_argument(
        "--weather-output",
        type=Path,
        default=Path("source/cbc_website/CAPC/CAPC_weather.csv"),
        help="Weather output CSV",
    )
    parser.add_argument("--sheet", default=0)
    parser.add_argument("--stop-species", default="House Sparrow")

    args = parser.parse_args()

    # Extract counts first (without writing) so we can derive the filename from
    # the actual year range in the sheet.
    out = extract_counts(
        args.input,
        None,
        sheet_name=args.sheet,
        stop_species=args.stop_species,
    )
    year_cols = [c for c in out.columns if c != "Species"]
    years = [int(c) for c in year_cols]
    first_year, last_year = min(years), max(years)
    counts_output = args.counts_output
    if counts_output is None:
        counts_output = Path(f"source/cbc_website/CAPC/CAPC_{first_year}_{last_year}.csv")
    counts_output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(counts_output, index=False)

    # Participants + effort + weather
    pe = extract_participants_effort(args.input, sheet_name=args.sheet)
    weather = extract_weather(args.input, sheet_name=args.sheet)
    meta = extract_year_header_metadata(args.input, sheet_name=args.sheet)

    # Join mapping info where possible
    if "CountIndex" in weather.columns:
        # Prefer the participants/effort table (it has filled participant counts),
        # but fall back to the header metadata for years not present there (e.g., 2024 [125]).
        if "CountIndex" in pe.columns:
            weather = weather.merge(pe[["CountIndex", "Year", "CountDate"]], on="CountIndex", how="left")
        if "CountIndex" in meta.columns:
            weather = weather.merge(
                meta[["CountIndex", "Year", "CountDate"]].rename(
                    columns={"Year": "Year_meta", "CountDate": "CountDate_meta"}
                ),
                on="CountIndex",
                how="left",
            )
            weather["Year"] = weather["Year"].fillna(weather["Year_meta"])
            weather["CountDate"] = weather["CountDate"].fillna(weather["CountDate_meta"])
            weather = weather.drop(columns=[c for c in ["Year_meta", "CountDate_meta"] if c in weather.columns])
        # Put Year/CountDate up front
        cols = ["Year", "CountDate", "CountIndex"] + [c for c in weather.columns if c not in {"Year", "CountDate", "CountIndex"}]
        weather = weather[cols]

    # Participants/effort tables: keep requested fields in a clean format
    participants = pe[["Year", "CountDate", "CountIndex", "NumParticipants"]].copy()
    effort = pe[["Year", "CountDate", "CountIndex", "NumHours"]].copy()

    # Ensure Year is written as an integer column (no decimals in CSV).
    if "Year" in participants.columns:
        participants["Year"] = pd.to_numeric(participants["Year"], errors="coerce").astype("Int64")
    if "Year" in effort.columns:
        effort["Year"] = pd.to_numeric(effort["Year"], errors="coerce").astype("Int64")
    if "Year" in weather.columns:
        weather["Year"] = pd.to_numeric(weather["Year"], errors="coerce").astype("Int64")

    args.participants_output.parent.mkdir(parents=True, exist_ok=True)
    participants.to_csv(args.participants_output, index=False)
    args.effort_output.parent.mkdir(parents=True, exist_ok=True)
    effort.to_csv(args.effort_output, index=False)
    args.weather_output.parent.mkdir(parents=True, exist_ok=True)
    weather.to_csv(args.weather_output, index=False)

    print(f"Wrote {counts_output} ({out.shape[0]} rows, {out.shape[1]} cols); years {first_year}-{last_year}")
    print(f"Wrote {args.participants_output} ({participants.shape[0]} rows)")
    print(f"Wrote {args.effort_output} ({effort.shape[0]} rows)")
    print(f"Wrote {args.weather_output} ({weather.shape[0]} rows)")


if __name__ == "__main__":
    main()
