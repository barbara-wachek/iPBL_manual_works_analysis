import gspread as gs
from gspread_dataframe import get_as_dataframe
import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor


# %% CONNECT GOOGLE (tylko do odczytu)

gc = gs.oauth()


# %% FUNCTIONS

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
        return name, url, "INVALID_URL", None

    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(clean_link, timeout=10, headers=headers)

        status = r.status_code

        soup = BeautifulSoup(r.text, 'html.parser')
        title = soup.title.get_text(strip=True) if soup.title else "BRAK_TYTUŁU"

        return name, clean_link, status, title

    except requests.RequestException:
        return name, clean_link, "ERROR", "BŁĄD"


# %% MAIN

gsheet_id = "1nw7bwi15rm-0BAc_Hf47PX_qBEPV1Wjwhn1-mpebyx4"
worksheet_name = "Do katalogu 4000"

df = gsheet_to_df(gsheet_id, worksheet_name)

# upewnij się, że kolumny istnieją
df = df[['Nazwa', 'Adres']].copy()

# THREADING
with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(executor.map(check_single, df.to_dict('records')))

# DATAFRAME
result_df = pd.DataFrame(results, columns=["Nazwa", "Adres", "Status", "Tytul strony"])

# sanity check
assert len(df) == len(result_df), "Mismatch between input and output!"

# %% ZAPIS DO EXCELA

output_path = "data/results.xlsx"
result_df.to_excel(output_path, index=False)

print(f"✅ Zapisano do: {output_path}")




