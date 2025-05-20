import pathlib
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_formatting import CellFormat, TextFormat, format_cell_range

ROOT_DIR = pathlib.Path(__file__).resolve().parent
KEY_PATH  = ROOT_DIR / "service_key.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(KEY_PATH, scopes=SCOPES)
gc    = gspread.authorize(creds)

SPREAD_ID  = "1SM1IaPZiVGrOwvREzG9nOTEBwLRfdbkVMsbn2Cfw1Jw"
SRC_SHEET  = "Архив новостей (исходный формат)"
TARGETS    = ["M2M", "UC", "Связь для бизнеса", "Конвергентные продукты для бизнеса"]

sh       = gc.open_by_key(SPREAD_ID)
ws_src   = sh.worksheet(SRC_SHEET)
df       = get_as_dataframe(ws_src, dtype=str, header=0)

df["Отметка времени"] = pd.to_datetime(df["Отметка времени"], dayfirst=True, errors="coerce")
df["Номер недели"]    = df["Отметка времени"].dt.isocalendar().week

system_cols = ["Отметка времени", "Направление", "Номер недели"]
news_cols   = [c for c in df.columns if c not in system_cols]

long_df = (
    df.melt(
        id_vars=["Номер недели", "Направление"],
        value_vars=news_cols,
        var_name="source_col",
        value_name="Новость"
    )
    .dropna(subset=["Новость"])
)

long_df["Статус"] = (
    long_df["source_col"]
      .str.replace(r"Новость\s*-\s*", "", regex=True)
      .str.replace(r"\.\d+$", "", regex=True)
      .str.strip()
)

long_df = (
    long_df.loc[:, ["Номер недели", "Направление", "Новость", "Статус"]]
           .sort_values(["Номер недели", "Новость"])
           .reset_index(drop=True)
)

bold_fmt = CellFormat(textFormat=TextFormat(bold=True))
wrap_fmt = CellFormat(wrapStrategy="WRAP")

for direction in TARGETS:
    sub = (
        long_df[long_df["Направление"] == direction]
        .drop(columns="Направление")      
        .reset_index(drop=True)
    )

    if sub.empty:
        sub = pd.DataFrame(columns=["Номер недели", "Новость", "Статус"])

    try:
        ws_tgt = sh.worksheet(direction)
        ws_tgt.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws_tgt = sh.add_worksheet(title=direction, rows="1000", cols="6")

    set_with_dataframe(ws_tgt, sub, include_index=False, include_column_header=True)
    ws_tgt.freeze(rows=1)

    format_cell_range(ws_tgt, "A1:C1", bold_fmt)
    format_cell_range(ws_tgt, "A:C", wrap_fmt)
