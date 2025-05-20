from __future__ import annotations

import pathlib
import time
from typing import List

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_formatting import (
    CellFormat,
    Color,
    TextFormat,
    batch_updater,
    format_cell_range,
    set_column_width,
)
from gspread.utils import rowcol_to_a1

ROOT_DIR = pathlib.Path(__file__).resolve().parent
KEY_PATH = ROOT_DIR / "service_key.json"

SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)

SPREAD_ID = "1SM1IaPZiVGrOwvREzG9nOTEBwLRfdbkVMsbn2Cfw1Jw"
SRC_SHEET = "Архив новостей (исходный формат)"

TARGET_SHEETS: List[str] = [
    "M2M",
    "UC",
    "Связь для бизнеса",
    "Конвергентные продукты для бизнеса",
]

COL_WIDTHS = {1: 100, 2: 1700, 3: 160}

COLOR_PALETTE = [
    Color(0.95, 0.95, 0.95),
    Color(0.87, 0.94, 0.98),
    Color(0.98, 0.90, 0.90),
    Color(0.90, 0.96, 0.87),
    Color(0.99, 0.95, 0.86),
    Color(0.93, 0.88, 0.98),
]

BOLD_HDR = CellFormat(textFormat=TextFormat(bold=True))
NO_WRAP = CellFormat(wrapStrategy="OVERFLOW_CELL")

creds = Credentials.from_service_account_file(KEY_PATH, scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREAD_ID)


def get_or_create_sheet(title: str, rows: int = 1000, cols: int = 10) -> gspread.Worksheet:  
    """Return existing worksheet or create a new one if missing."""
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))


def zebra_ranges(row_count: int, week_series: pd.Series) -> List[tuple[str, CellFormat]]:
    """Return list of (A‑C range, CellFormat) tuples for zebra colour blocks."""
    ranges: List[tuple[str, CellFormat]] = []
    if week_series.empty:
        return ranges

    start_row = 2
    current_week = week_series.iloc[0]
    colour_idx = 0

    for idx, week in enumerate(week_series, start=2):  
        if week != current_week:
            ranges.append((f"A{start_row}:C{idx-1}", CellFormat(backgroundColor=COLOR_PALETTE[colour_idx])))
            start_row = idx
            current_week = week
            colour_idx = (colour_idx + 1) % len(COLOR_PALETTE)

    ranges.append((f"A{start_row}:C{row_count+1}", CellFormat(backgroundColor=COLOR_PALETTE[colour_idx])))
    return ranges

ws_src = sh.worksheet(SRC_SHEET)

df = get_as_dataframe(ws_src, dtype=str).dropna(how="all")

df["Отметка времени"] = pd.to_datetime(df["Отметка времени"], dayfirst=True)
df["Неделя"] = df["Отметка времени"].dt.isocalendar().week

id_vars = ["Неделя", "Направление"]
value_vars = [c for c in df.columns if c not in ["Отметка времени", *id_vars]]

melted = (
    df.melt(id_vars=id_vars, value_vars=value_vars, var_name="source", value_name="Новость")
    .dropna(subset=["Новость"])
)

melted["Статус"] = (
    melted["source"]
    .str.replace(r"Новость\s*-\s*", "", regex=True)
    .str.replace(r"\.\d+$", "", regex=True)
    .str.strip()
)

final_df = (
    melted.loc[:, ["Неделя", "Направление", "Новость", "Статус"]]
    .rename(columns={"Неделя": "Номер недели"})
    .sort_values(["Номер недели", "Новость"])
)

for direction in TARGET_SHEETS:
    sheet_df = (
        final_df[final_df["Направление"] == direction]
        .drop(columns="Направление")
        .reset_index(drop=True)
    )

    if sheet_df.empty:
        sheet_df = pd.DataFrame(columns=["Номер недели", "Новость", "Статус"])

    ws = get_or_create_sheet(direction)
    ws.clear()
    set_with_dataframe(ws, sheet_df, include_index=False, include_column_header=True)
    ws.freeze(rows=1)

    weeks_series = sheet_df["Номер недели"] if not sheet_df.empty else pd.Series(dtype=int)

    try:
        with batch_updater(sh):
            format_cell_range(ws, "A1:C1", BOLD_HDR)
            format_cell_range(ws, "A:C", NO_WRAP)

            for rng, fmt in zebra_ranges(len(sheet_df), weeks_series):
                format_cell_range(ws, rng, fmt)

            for idx, width in COL_WIDTHS.items():
                set_column_width(ws, rowcol_to_a1(1, idx)[:-1], width)

    except gspread.exceptions.APIError as err:
        if "Must specify at least one request" not in str(err):
            raise

    time.sleep(0.5)
