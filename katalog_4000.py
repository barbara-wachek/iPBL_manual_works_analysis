import gspread as gs
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
import requests
import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


#%% connect google drive

#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)


#%% functions
def gsheet_to_df(gsheetId, worksheet):
    sheet = gc.open_by_key(gsheetId)
    df = get_as_dataframe(
        sheet.worksheet(worksheet),
        evaluate_formulas=True,
        dtype=str
    )

    df = df.dropna(how='all').dropna(axis=1, how='all')
    return df



def clean_url(url):
    if not url:
        return None

    url = str(url).strip()

    if not url:
        return None

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    return url



def check_single(row):
    name = row['Nazwa']
    url = row['Adres']

    clean_link = clean_url(url)

    if not clean_link:
        return name, url, "INVALID_URL"

    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.head(clean_link, timeout=10, allow_redirects=True, headers=headers)

        if r.status_code == 405:
            r = requests.get(clean_link, timeout=10, headers=headers)

        return name, clean_link, r.status_code

    except requests.RequestException:
        return name, clean_link, "ERROR"
    
#%% main

gsheet_id = "1nw7bwi15rm-0BAc_Hf47PX_qBEPV1Wjwhn1-mpebyx4"
worksheet_name = "Do katalogu 4000"

df = gsheet_to_df(gsheet_id, worksheet_name)

df = df[['Nazwa', 'Adres']].copy()

results = []

with ThreadPoolExecutor(max_workers=20) as executor:
    results = list(executor.map(check_single, df.to_dict('records')))

result_df = pd.DataFrame(results, columns=["nazwa", "url", "status"])


# sanity check
assert len(df) == len(result_df), "Mismatch between input and output!"


#%% EXCEL


result_df.to_excel("data/results.xlsx", index=False)




 #%% WRITE BACK TO GOOGLE SHEETS

# sheet = gc.open_by_key(gsheet_id).worksheet(worksheet_name)

# header = sheet.row_values(1)
# status_col = header.index("Status") + 1

# # sortujemy po row (ważne!)
# result_df = result_df.sort_values("row")

# values = [[x] for x in result_df["status"]]

# start_row = 2
# end_row = start_row + len(values) - 1

# from gspread.utils import rowcol_to_a1
# col_letter = rowcol_to_a1(1, status_col)[0]

# sheet.update(
#     f"{col_letter}{start_row}:{col_letter}{end_row}",
#     values
# )




#%% SANITY CHECKS



check_df = result_df.sort_values("row")

sheet_status = sheet.col_values(status_col)[1:]

for i, (_, row, _, status) in enumerate(check_df.values):
    sheet_val = sheet_status[i]

    if str(sheet_val) != str(status):
        print("MISMATCH!")
        print("row:", row)
        print("df:", status)
        print("sheet:", sheet_val)
        break




import random

sample = random.sample(range(len(result_df)), 10)

for i in sample:
    row = result_df.iloc[i]["row"]
    status = result_df.iloc[i]["status"]

    sheet_status = sheet.cell(int(row)+1, status_col).value

    print(row, status, sheet_status)





