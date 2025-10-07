from tqdm import tqdm
from datetime import datetime
from gspread.exceptions import WorksheetNotFound
import pandas as pd
import gspread as gs
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import regex as re
import numpy as np
import requests
import calendar
from collections import Counter

from oauth2client.service_account import ServiceAccountCredentials
import time


#%% connect google drive

#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)


#%% functions
def gsheet_to_df(gsheetId, worksheet):
    gc = gs.oauth()
    sheet = gc.open_by_key(gsheetId)
    df = get_as_dataframe(sheet.worksheet(worksheet), evaluate_formulas=True, dtype=str).dropna(how='all').dropna(how='all', axis=1)
    return df


#%% Przerobiłam kod do statystyk. Nie uwzględniam KP i PCL - bo one są przydzielone do prac półautomatycznych, a chcemy oszacowac jak przebiegały prace manualne w ostatnim czasie, żeby oszacować prace przez kolejne 7 miesięcy

#Założenia: RM pracuje do czerwca. Niektorzy juz zrobili przewidziane rekordy, wiec nie mozemy oczekiwac ze beda pracowac tak samo ciezko? (AW?)


file_list = drive.ListFile({'q': "'1ZrLyjsA6Q-k78M8gpuK5EB2NXCk56zA0' in parents and trashed=false"}).GetList()

excluded_names = {'test', 'BK', 'KP', '.iPBL – statystyki rekordów do raportów rocznych'}

# Filtrowanie plików
filtered_files = [f for f in file_list if f['title'] not in excluded_names]

# Słownik na DataFrame’y
stats_dfs = {}

# Iteracja po plikach
for f in tqdm(filtered_files):
    file_id = f['id']
    title = f['title']

    try:
        sheet = gc.open_by_key(file_id)
        worksheet = sheet.get_worksheet(0)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)

        if 'data' in df.columns and 'jest rekordów' in df.columns:
            selected_df = df[['data', 'jest rekordów']].copy()
            stats_dfs[title] = selected_df
            print(f"✔ Dodano do słownika: {title}")
        else:
            print(f"⚠ W pliku {title} brakuje wymaganych kolumn")

    except Exception as e:
        print(f"❌ Błąd przy przetwarzaniu pliku {title}: {e}")


# --- Filtrowanie danych z 2025 roku ---
filtered_2025_dfs = {}

for title, df in stats_dfs.items():
    try:
        df['data'] = pd.to_datetime(df['data'], errors='coerce')
        df_2025 = df[df['data'].dt.year == 2025].copy()
        filtered_2025_dfs[title] = df_2025
        print(f"✔ {title}: {df_2025.shape[0]} wierszy z 2025 roku (zachowano pełne dane)")
    except Exception as e:
        print(f"❌ Błąd przy filtrowaniu danych z 2025 roku w {title}: {e}")


# --- Obliczanie średnich miesięcznych przyrostów ---
monthly_growth_rates = {}

for title, df in filtered_2025_dfs.items():
    try:
        df = df.sort_values(by='data').reset_index(drop=True)
        df['record_diff'] = df['jest rekordów'].diff()
        df['days_diff'] = df['data'].diff().dt.days
        df['monthly_growth'] = (df['record_diff'] / df['days_diff']) * 30
        df_clean = df.dropna(subset=['monthly_growth'])
        avg_monthly_growth = df_clean['monthly_growth'].mean()
        monthly_growth_rates[title] = avg_monthly_growth
        print(f"✔ {title}: średni miesięczny przyrost rekordów w 2025 roku = {avg_monthly_growth:.2f}")
    except Exception as e:
        print(f"❌ Błąd przy obliczaniu miesięcznego przyrostu dla {title}: {e}")


# --- Prognozy ---
inactive_workers = {'RM'}
months_total = 4
forecast_records = {}

for worker, monthly_rate in monthly_growth_rates.items():
    if worker in inactive_workers:
        print(f"⏸ {worker}: pominięty w prognozie (nieaktywny pracownik, ale dane historyczne zachowane)")
        continue

    # Standardowy sposób obliczania prognozy dla wszystkich aktywnych pracowników
    forecast = monthly_rate * months_total
    forecast_records[worker] = forecast

    print(f"➡ {worker}: prognozowana liczba rekordów za kolejne {months_total} miesiące = {forecast:.0f}")

total_forecast = sum(forecast_records.values())
print(f"\n🧮 Łączna prognozowana liczba rekordów od aktywnych pracowników (bez RM): {total_forecast:.0f}")


# --- Szacunki z arkusza ---
szacunki = {
    'AW': 4045.13, 'BD': 3526.95, 'BL': 4045.13,
    'EP': 3627.25, 'PCL': 1604.68, 'MSz': 3363,
    'RM': 15996.66, 'IH': 702
}


# --- Realizacje (ostatnie wartości) ---
realizacje = {}

for person, df in filtered_2025_dfs.items():
    try:
        df['data'] = pd.to_datetime(df['data'], errors='coerce')
        df = df.dropna(subset=['data'])
        df_sorted = df.sort_values(by='data')
        latest_record = df_sorted['jest rekordów'].iloc[-1]
        realizacje[person] = latest_record
    except Exception as e:
        print(f"⚠️ Błąd przy przetwarzaniu {person}: {e}")


# --- Porównanie realizacji względem celów ---
print("\n📊 Porównanie postępu pracowników względem założonych celów:\n")

for person, cel in szacunki.items():
    zrealizowano = realizacje.get(person, 0)
    procent = (zrealizowano / cel) * 100 if cel else 0
    print(f"{person}: {zrealizowano:.0f} / {cel:.0f} rekordów ({procent:.1f}%)")


# --- PODSUMOWANIE: realizacje + prognozy ---
print("\n📈 Prognoza końcowa (realizacje + prognozy):\n")

summary = []
all_workers = set(realizacje.keys()) | set(forecast_records.keys())

for worker in sorted(all_workers):
    done = realizacje.get(worker, 0)
    forecast = forecast_records.get(worker, 0)
    total_estimated = done + forecast
    note = " (bez prognozy — zakończył pracę)" if worker in inactive_workers else ""
    goal = szacunki.get(worker)
    if goal:
        percent_of_goal = (total_estimated / goal) * 100
        percent_text = f"{percent_of_goal:.1f}%"
    else:
        percent_text = "—"
    print(f"{worker}: {done:.0f} + {forecast:.0f} = {total_estimated:.0f} rekordów{note} ({percent_text} celu)")
    summary.append(total_estimated)

grand_total = sum(summary)
print(f"\n📊 Szacowana łączna liczba rekordów (realizacje + prognozy): {grand_total:.0f}")



