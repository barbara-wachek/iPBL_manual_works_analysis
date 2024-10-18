#%% 
from tqdm import tqdm
from gspread.exceptions import WorksheetNotFound
from datetime import datetime, date, timedelta
import pandas as pd
import gspread as gs
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from collections import Counter
import calendar
import regex as re
import time


#%% 
#Czynnosci: (potem przeniesc to do pliku readme)
    #Pobrac wszystkie przeanalizowane tabele (automatycznie) na podstawie jakiegos warunku? 
    #Połączyć te tabele w jeden DataFrame
    #Namierzyć te komórki z kolumny Autor które mają pipe (|). Sprawdzić ilu autorów występuje w tych polach 
    #Jako klucz traktować Link  + pola autor + VIAF
    #Wynikiem ma być plik JSON
    #jeśli liczba autorów i liczba viafów się zgadza – nie ma problemu
    #problemy:
        # jeśli liczba autorów jest różna od liczby przypisanych viafów, gdy liczba viafów =! 0
        # jeśli po splicie (|) w labelu autora jest więcej niż 1 spacja → do zastosowania też dla pojedynczych autorów
        # Basia liczy i decydujemy, co zrobić dalej
        # deadline – koniec listopada
        # pipeline do powtórzenia pod koniec projektu


#Najpierw wchodzę w plik: Dokumentacja (prace manualne) i pobieram te linki, które są opisane jako zakończone (kolumna: STATUS PRAC) i pobieram link z kolumny LINK DO ARKUSZA


#%%
def gsheet_to_df(gsheetId, worksheet):
    gc = gs.oauth()
    sheet = gc.open_by_key(gsheetId)
    df = get_as_dataframe(sheet.worksheet(worksheet), evaluate_formulas=True, dtype=str).dropna(how='all').dropna(how='all', axis=1)
    return df


#%% connect google drive

#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)


dokumentacja_df = gsheet_to_df('1jCjEaopxsezprUiauuYkwQcG1cp40NqdhvxIzG5qUu8', 'dokumentacja')
new_df = dokumentacja_df.loc[dokumentacja_df['STATUS PRAC'] == 'zakończono'] 
links_of_finished_servies = new_df['LINK'].tolist()


# all_finished_servies_df = pd.DataFrame()
all_tabels = []
for link in tqdm(links_of_finished_servies):
    gsheetId = re.search(r'(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\-\d\_]*', link).group(0)
    all_tabels.append(gsheet_to_df(gsheetId, worksheet='Posts'))
    

all_finished_servies_df = pd.concat(all_tables)




























