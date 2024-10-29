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
    #Pobrac wszystkie przeanalizowane tabele (automatycznie) na podstawie jakiegos warunku? Status prac = zakończono lub przerwano (Czy brac też rozpoczęto?)
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
new_df = dokumentacja_df.loc[(dokumentacja_df['STATUS PRAC'] == 'zakończono') | (dokumentacja_df['STATUS PRAC'] == 'przerwano')] #PAMIETAC ZEBY UWZGLEDNIC STATUS ROZPOCZETO (To okolo 20 tabel wiecej)
tables_links = new_df['LINK'].tolist()


# all_tables_df = pd.DataFrame()
all_tables = []
for link in tqdm(tables_links):
    gsheetId = re.search(r'(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\-\d\_]*', link).group(0)
    df = gsheet_to_df(gsheetId, worksheet='Posts')
    df = df.loc[df['Link'].notna()]
    all_tables.append(df)
    
 
#Łączenie wszystkich pobranych tabel w jeden DataFrame:     
all_tables_df = pd.concat(all_tables, axis=0)  #58451 rekordów (zakonczono i przerwano)  # 128784(zakonczono, przerwano, rozpoczeto)
all_tables_df.columns

#Z połączonych tabel wybrac tylko te, ktore w kolumnie "do PBL" maja wartosc True
all_tables_df_only_accepted_records = all_tables_df.loc[all_tables_df['do PBL'] == 'True'] #8287 #17701 (zakonczono, przerwano, rozpoczeto)


selected_columns = ['Link', 'Autor', 'VIAF autor 1', 'Autor wiersza', 'VIAF autor 2', 'VIAF autor 3']
df_new = all_tables_df_only_accepted_records [selected_columns]


# df_new_with_notna_authors = df_new.loc[(df_new['Autor'].notna())]

# df_new_with_pipe = df_new_with_notna_authors.loc[(df_new_with_notna_authors['Autor'].str.contains(' | ')) | (df_new_with_notna_authors['Autor dzieła'].str.contains(' | '))]


df_records_with_few_authors = df_new[
    df_new['Autor'].str.contains(r'\s*\|\s*') | 
    df_new['Autor wiersza'].str.contains(r'\s*\|\s*') |
    df_new['Autor'].str.contains(r'\,') |
    df_new['Autor wiersza'].str.contains(r'\,')
]

#76 takich wierszy wyszło (pipe'a praktycznie nie ma, raczej przecinek)

for index, row in df_records_with_few_authors.iterrows():
    try:
        split_authors = row['Autor'].split(',')
        df_records_with_few_authors.at[index, 'Autor'] = split_authors
    except:
        print(f'Jakis blad w wierszu {index}')



for index, row in df_records_with_few_authors.iterrows():
    try:
        split_authors = row['Autor'].split(',')
        row['Autor'] = split_authors  # Modify the row Series
        df_records_with_few_authors.iloc[index] = row  # Assign the modified row back
    except:
        print(f'Jakis blad w wierszu {index}')

# raport_df = {
#     'Index': []
#     'Opis': [] 
#     }



raport_df = {}




for index, row in df_records_with_few_authors.iterrows():
    number_of_authors = len(row['Autor'])
    if number_of_authors == 2:
        if row['VIAF autor 1'] != 'nan' and row['VIAF autor 2'] != 'nan':
            raport_df[index] = 'OK'
        else:
            report_df[index] = 'Różnica'
    
    if number_of_authors == 3:
        if row['VIAF autor 1'] != 'nan' and row['VIAF autor 2'] != 'nan' and row['VIAF autor 3'] != 'nan':
            raport_df[index] = 'OK'
        else:
            report_df[index] = 'Różnica'


# czyszczenie danych 
# OK     pobranie wszystich danych
# OK    filtrowanie tych, które w polu autor mają separator (|) - raczej ',' występuje.| nie był tu używany
# OK    splitujemy pole Autor
# jeśli liczba autorów i liczba viafów się zgadza – nie ma problemu
# problemy:
# jeśli liczba autorów jest różna od liczby przypisanych viafów, gdy liczba viafów =! 0
# jeśli po splicie (|) w labelu autora jest więcej niż 1 spacja → do zastosowania też dla pojedynczych autorów
# Basia liczy i decydujemy, co zrobić dalej
# deadline – koniec listopada
# pipeline do powtórzenia pod koniec projektu





#Naprawa błędów: 
    
df_bug = df_new.loc[df_new['...'].notna()]
df_bug = df_new.loc[df_new['VIAF autor 2David Hal'].notna()]


Kolejne kroki: wyciagnac z df tylko kolumny Link, Autor i VIAF (autor 1, autor 2 itd.)



all_tables_df = pd.merge(all_tables, how='left')






























