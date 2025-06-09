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
    title = f['title']  # Używamy oryginalnej nazwy pliku jako klucza

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
        

filtered_2025_dfs = {}

for title, df in stats_dfs.items():
    try:
        # Konwersja kolumny "data" do datetime
        df['data'] = pd.to_datetime(df['data'], errors='coerce')

        # Filtrowanie tylko dat z 2025 roku
        df_2025 = df[df['data'].dt.year == 2025].copy()

        # Zachowujemy przefiltrowany DataFrame w słowniku
        filtered_2025_dfs[title] = df_2025

        print(f"✔ {title}: {df_2025.shape[0]} wierszy z 2025 roku (zachowano pełne dane)")

    except Exception as e:
        print(f"❌ Błąd przy filtrowaniu danych z 2025 roku w {title}: {e}")


monthly_growth_rates = {}  # słownik na średnie przyrosty miesięczne dla każdego pracownika

for title, df in filtered_2025_dfs.items():
    try:
        # Sortujemy po dacie
        df = df.sort_values(by='data').reset_index(drop=True)
        
        # Obliczamy różnice w liczbie rekordów (przyrosty)
        df['record_diff'] = df['jest rekordów'].diff()
        
        # Obliczamy różnicę w dniach między pomiarami
        df['days_diff'] = df['data'].diff().dt.days
        
        # Obliczamy miesięczny przyrost: (przyrost / liczba dni) * 30
        df['monthly_growth'] = (df['record_diff'] / df['days_diff']) * 30
        
        # Usuwamy wiersze z NaN (pierwszy wiersz oraz ewentualne błędy)
        df_clean = df.dropna(subset=['monthly_growth'])
        
        # Obliczamy średnią miesięczną prędkość pracy (średni przyrost rekordów na miesiąc)
        avg_monthly_growth = df_clean['monthly_growth'].mean()
        
        monthly_growth_rates[title] = avg_monthly_growth
        
        print(f"✔ {title}: średni miesięczny przyrost rekordów w 2025 roku = {avg_monthly_growth:.2f}")

    except Exception as e:
        print(f"❌ Błąd przy obliczaniu miesięcznego przyrostu dla {title}: {e}")



# Inicjały pracownika, który pracuje jeszcze 1 miesiąc
special_worker = 'RM'

# Liczba miesięcy pracy w prognozie
months_total = 7
months_special_worker = 1
months_others = months_total - months_special_worker

# Słownik na prognozowaną liczbę rekordów do dopisania
forecast_records = {}

for worker, monthly_rate in monthly_growth_rates.items():
    #Wartosc dla IH przyjete na sztywno
    if worker == 'IH': 
        forecast = 702
        months = months_total
        forecast_records[worker] = forecast
        
    # Prognozowana liczba rekordów w tych miesiącach
    else:
        months = months_special_worker if worker == special_worker else months_total
        forecast = monthly_rate * months
        forecast_records[worker] = forecast
    
    print(f"➡ {worker}: prognozowana liczba rekordów za kolejne {months} miesięcy = {forecast:.0f}")

# Podsumowanie
total_forecast = sum(forecast_records.values())
print(f"\n🧮 Łączna prognozowana liczba rekordów od wszystkich pracowników: {total_forecast:.0f}")


#Na podstawie pliku: https://docs.google.com/spreadsheets/d/1fZxyEYxGPsGfaMGXUFYaCrTAgxV40Yi4-vzgIsyU9LA/edit?gid=199726957#gid=199726957 

szacunki = {'AW': 4045.13, 'BD': 3526.95, 'BL': 4045.13, 'EP': 3627.25, 'PCL': 1604.68, 'MSz': 3109.07, 'RM': 15996.66, 'IH': 702}

realizacje = {}

for person, df in filtered_2025_dfs.items():
    try:
        # Upewnij się, że 'data' to kolumna dat
        df['data'] = pd.to_datetime(df['data'], errors='coerce')
        df = df.dropna(subset=['data'])  # usuń wiersze bez daty

        # Sortuj rosnąco po dacie
        df_sorted = df.sort_values(by='data')

        # Pobierz ostatnią wartość z kolumny "jest rekordów"
        latest_record = df_sorted['jest rekordów'].iloc[-1]
        realizacje[person] = latest_record
    except Exception as e:
        print(f"⚠️ Błąd przy przetwarzaniu {person}: {e}")

# Porównanie z szacunkami
print("\n📊 Porównanie postępu pracowników względem założonych celów:\n")

for person, cel in szacunki.items():
    zrealizowano = realizacje.get(person, 0)
    procent = (zrealizowano / cel) * 100 if cel else 0
    print(f"{person}: {zrealizowano:.0f} / {cel:.0f} rekordów ({procent:.1f}%)")






