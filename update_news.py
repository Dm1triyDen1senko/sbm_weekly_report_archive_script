import json, os, tempfile, pathlib
import pandas as pd, gspread

from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_formatting import format_cell_range, CellFormat, TextFormat


# start of workflow credentials block

ROOT_DIR = pathlib.Path(__file__).resolve().parent      
KEY_PATH = ROOT_DIR / "service_key.json"               

scope = ["https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive"]

creds = Credentials.from_service_account_file(KEY_PATH, scopes=scope)
gc = gspread.authorize(creds)

# end of workflow credentials block


SPREAD_ID = "1SM1IaPZiVGrOwvREzG9nOTEBwLRfdbkVMsbn2Cfw1Jw"           
SRC_SHEET = "Архив новостей (исходный формат)"

sh   = gc.open_by_key(SPREAD_ID)
ws   = sh.worksheet(SRC_SHEET)
df   = get_as_dataframe(ws, dtype=str, header=0)  

df["Отметка времени"] = pd.to_datetime(df["Отметка времени"], dayfirst=True)

df["Неделя"] = df["Отметка времени"].dt.isocalendar().week


system_cols = ["Отметка времени", "Направление", "Неделя"]   
news_cols   = [c for c in df.columns if c not in system_cols]

melted = (
    df.melt(id_vars=["Неделя", "Направление"], value_vars=news_cols, value_name="Новость")
      .dropna(subset=["Новость"])                    
      .sort_values(["Неделя", "Новость"])
      .loc[:, ["Неделя", "Направление", "Новость"]]    
)


targets = ["M2M", "UC", "Связь для бизнеса", "Конвергентные продукты для бизнеса"]

for direction in targets:
    sub = melted[melted["Направление"] == direction].reset_index(drop=True)

    try:
        w = sh.worksheet(direction)
        w.clear()
    except gspread.exceptions.WorksheetNotFound:
        w = sh.add_worksheet(title=direction, rows="1000", cols="10")

    set_with_dataframe(w, sub, include_index=False, include_column_header=True)

    w.freeze(rows=1)


    targets = ["M2M", "UC", "Связь для бизнеса", "Конвергентные продукты для бизнеса"]

for title in targets:
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Лист «{title}» не найден — пропускаю")
        continue

    header = ws.acell('A1').value
    if header == "Номер недели":
        continue

    if header == "Неделя":
         ws.update_acell('A1', 'Номер недели') 
    else:
        print(f"На «{title}» в A1 не «Неделя», а «{header}» — оставил без изменений")


id_vars   = ["Неделя", "Направление"]
value_vars = [c for c in df.columns if c not in ["Отметка времени", "Направление", "Неделя"]]

melted_s = (
    df.melt(id_vars=id_vars, value_vars=value_vars,
            var_name="source_col", value_name="Новость")
      .dropna(subset=["Новость"])
)

melted_s["Статус"] = (
    melted_s["source_col"]
      .str.replace(r"Новость\s*-\s*", "", regex=True)
      .str.replace(r"\.\d+$", "", regex=True)
      .str.strip()
)

melted_s = (
    melted_s.loc[:, ["Неделя", "Направление", "Новость", "Статус"]]
             .rename(columns={"Неделя": "Номер недели"})
             .sort_values(["Номер недели", "Новость"])
             .reset_index(drop=True)
)

targets = ["M2M", "UC", "Связь для бизнеса", "Конвергентные продукты для бизнеса"]

for direction in targets:
    sub = melted_s[melted_s["Направление"] == direction].reset_index(drop=True)

    if sub.empty:        
        sub = pd.DataFrame(columns=["Номер недели", "Направление", "Новость", "Статус"])

    try:
        w = sh.worksheet(direction)
        w.clear()
    except gspread.exceptions.WorksheetNotFound:
        w = sh.add_worksheet(title=direction, rows="1000", cols="6")

    set_with_dataframe(w, sub, include_index=False, include_column_header=True)
    w.freeze(rows=1)


targets = ["M2M", "UC", "Связь для бизнеса", "Конвергентные продукты для бизнеса"]

bold_hdr = CellFormat(textFormat=TextFormat(bold=True))

for title in targets:
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Лист «{title}» не найден — пропускаю")
        continue

    format_cell_range(ws, 'A1:D1', bold_hdr)


from gspread_formatting import CellFormat, format_cell_range

targets = ["M2M", "UC", "Связь для бизнеса", "Конвергентные продукты для бизнеса"]

wrap_fmt = CellFormat(wrapStrategy='WRAP')

for title in targets:
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Лист «{title}» не найден — пропускаю")
        continue

    format_cell_range(ws, 'A:D', wrap_fmt)
         

targets = ["M2M", "UC", "Связь для бизнеса", "Конвергентные продукты для бизнеса"]

for title in targets:
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Лист «{title}» не найден — пропускаю")
        continue

    headers = [h.strip().lower() for h in ws.row_values(1)]

    try:
        col_idx = headers.index("направление") + 1     
    except ValueError:
        print(f"На «{title}» столбца «Направление» нет — пропускаю")
        continue

    ws.delete_columns(col_idx)
    print(f"На «{title}» удалён столбец «Направление» (№{col_idx})")
