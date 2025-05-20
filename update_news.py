#!/usr/bin/env python3
"""Generate weekly news dashboards in Google Sheets.

Optimised version of the original script. All functionality is preserved,
with reduced duplication, clearer structure and a single pass over each
worksheet.
"""

from __future__ import annotations

import pathlib
from typing import List

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_formatting import (
    CellFormat,
    Color,
    TextFormat,
    format_cell_range,
    set_column_width,
)
from gspread.utils import rowcol_to_a1

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# AUTHENTICATION & SHEET HANDLE
# ---------------------------------------------------------------------------
creds = Credentials.from_service_account_file(KEY_PATH, scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREAD_ID)


def get_or_create_sheet(title: str, rows: int = 1000, cols: int = 10) -> gspread.Worksheet:  # noqa: D401,E501
    """Return existing worksheet or create a new one if missing."""
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))


# ---------------------------------------------------------------------------
# DATA PREPARATION
# ---------------------------------------------------------------------------
ws_src = sh.worksheet(SRC_SHEET)

df = (
    get_as_dataframe(ws_src, dtype=str)
    .dropna(how="all")
    .assign(
        **{
            "Отметка времени": lambda d: pd.to_datetime(d["Отметка времени"], dayfirst=True),
            "Неделя": lambda d: pd.to_datetime(d["Отметка времени"], dayfirst=True)
            .dt.isocalendar()
            .week,
        }
    )
)

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

# ---------------------------------------------------------------------------
# OUTPUT TO GOOGLE SHEETS
# ---------------------------------------------------------------------------
for direction in TARGET_SHEETS:
    sheet_df = final_df[final_df["Направление"] == direction].reset_index(drop=True)
    if sheet_df.empty:
        sheet_df = pd.DataFrame(columns=final_df.columns)

    ws = get_or_create_sheet(direction)
    ws.clear()
    set_with_dataframe(ws, sheet_df, include_index=False, include_column_header=True)
    ws.freeze(rows=1)

    # ------------------------ FORMATTING -----------------------------------
    # Bold header & disable wrap
    format_cell_range(ws, "A1:D1", BOLD_HDR)
    max_col_letter = rowcol_to_a1(1, ws.col_count)[:-1]
    format_cell_range(ws, f"A:{max_col_letter}", NO_WRAP)

    # Remove \"Направление\" column if present
    headers = [h.lower().strip() for h in ws.row_values(1)]
    if "направление" in headers:
        ws.delete_columns(headers.index("направление") + 1)

    # Keep only first 3 data columns
    data_cols = len(ws.row_values(1))
    for col in range(data_cols, 3, -1):
        ws.delete_columns(col)

    # Hide empty columns beyond C to keep view clean
    ws.unhide_columns(1, ws.col_count)
    if ws.col_count > 3:
        ws.hide_columns(4, ws.col_count)

    # Zebra colouring by week number
    weeks = ws.col_values(1)[1:]
    if weeks:
        block_start, prev_week, colour_idx = 2, weeks[0], 0
        for row, week in enumerate(weeks, start=2):
            if week != prev_week:
                rng = f"A{block_start}:C{row-1}"
                format_cell_range(ws, rng, CellFormat(backgroundColor=COLOR_PALETTE[colour_idx]))
                block_start, prev_week = row, week
                colour_idx = (colour_idx + 1) % len(COLOR_PALETTE)

        # Final block
        format_cell_range(
            ws,
            f"A{block_start}:C{len(weeks)+1}",
            CellFormat(backgroundColor=COLOR_PALETTE[colour_idx]),
        )

    # Column widths
    for idx, width in COL_WIDTHS.items():
        set_column_width(ws, rowcol_to_a1(1, idx)[:-1], width)

print("✔️  Google Sheets updated successfully.")
