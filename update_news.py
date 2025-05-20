import json, os, tempfile, pathlib
import pandas as pd, gspread

from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_formatting import format_cell_range, CellFormat, TextFormat, CellFormat, format_cell_range, Color, format_cell_range, CellFormat
from gspread.utils import rowcol_to_a1 


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
 

# targets = ["M2M", "UC", "Связь для бизнеса", "Конвергентные продукты для бизнеса"]

# wrap_fmt = CellFormat(wrapStrategy='WRAP')

# for title in targets:
#     try:
#         ws = sh.worksheet(title)
#     except gspread.exceptions.WorksheetNotFound:
#         print(f"Лист «{title}» не найден — пропускаю")
#         continue

#     format_cell_range(ws, 'A:D', wrap_fmt)

# -------------------------------------------------- отключить перенос строк
NO_WRAP_FMT = CellFormat(wrapStrategy='OVERFLOW_CELL')   # или 'CLIP'

for title in targets:
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        continue

    # сколько реально видимых столбцов осталось?
    col_count = len(ws.row_values(1))        # строка-заголовок
    last_col  = rowcol_to_a1(1, col_count)[:-1]   # «C» → последняя буква

    rng = f"A:{last_col}"                    # весь диапазон A…последний
    format_cell_range(ws, rng, NO_WRAP_FMT)

    print(f"На «{title}» перенос строк отключён (wrapStrategy={NO_WRAP_FMT.wrapStrategy})")
         

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


targets = ["M2M", "UC", "Связь для бизнеса", "Конвергентные продукты для бизнеса"]

for title in targets:
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Лист «{title}» не найден — пропускаю")
        continue

    total_cols = len(ws.row_values(1))  

    for col in range(total_cols, 3, -1): 
        ws.delete_columns(col)
      

targets = ["M2M", "UC", "Связь для бизнеса", "Конвергентные продукты для бизнеса"]

for title in targets:
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Лист «{title}» не найден — пропускаю")
        continue

    total_cols = ws.col_count                

    ws.unhide_columns(1, total_cols)

    if total_cols > 3:
        ws.hide_columns(4, total_cols)
        last_col_letter = rowcol_to_a1(1, total_cols)[:-1]  
        print(f"На «{title}» скрыты столбцы D:{last_col_letter}")
    else:
        print(f"На «{title}» только {total_cols} столбца — скрывать нечего")


COLOR_PALETTE = [
    Color(0.95, 0.95, 0.95),   
    Color(0.87, 0.94, 0.98),  
    Color(0.98, 0.90, 0.90),   
    Color(0.90, 0.96, 0.87),   
    Color(0.99, 0.95, 0.86),   
    Color(0.93, 0.88, 0.98),   
]

targets = ["M2M", "UC", "Связь для бизнеса", "Конвергентные продукты для бизнеса"]

for title in targets:
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Лист «{title}» не найден — пропускаю")
        continue

    weeks = ws.col_values(1)[1:]
    if not weeks:
        print(f"На «{title}» данных нет — пропускаю раскраску")
        continue

    color_idx   = 0
    block_start = 2               
    prev_week   = weeks[0]

    for row_offset, week in enumerate(weeks, start=2):
        if week != prev_week:
            block_end = row_offset - 1
            rng = f"A{block_start}:C{block_end}"
            fmt = CellFormat(backgroundColor=COLOR_PALETTE[color_idx])
            format_cell_range(ws, rng, fmt)

            block_start = row_offset
            prev_week   = week
            color_idx   = (color_idx + 1) % len(COLOR_PALETTE)

    rng = f"A{block_start}:C{len(weeks) + 1}"
    fmt = CellFormat(backgroundColor=COLOR_PALETTE[color_idx])
    format_cell_range(ws, rng, fmt)

    print(f"На «{title}» строки раскрашены по неделям")


# -------------------------------------------------- задать ширину столбцов
from gspread_formatting import set_column_width
from gspread.utils import rowcol_to_a1   # уже импортирован выше, но на всякий случай

# ширина в пикселях
COL_WIDTHS = {
    1: 100,     # A
    2: 1700,    # B
    3: 160,     # C
}

for title in ["M2M", "UC", "Связь для бизнеса", "Конвергентные продукты для бизнеса"]:
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Лист «{title}» не найден — пропускаю установку ширины")
        continue

    for idx, width in COL_WIDTHS.items():
        col_letter = rowcol_to_a1(1, idx)[:-1]   # 1→"A", 2→"B" …
        set_column_width(ws, col_letter, width)

    print(f"На «{title}» задана ширина столбцов A–C (100 / 1700 / 160)")
