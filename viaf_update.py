"""
#Uzupełnić tabele z opracowaniami manualnymi o VIAF
#Wybrać przykładową tabelę i napisać kod
#Dowiedzieć się od PCL do ktorych tabel to zastosowac - czy do wszystkich jeszcze nierozpoczętych? - 4 przykladowe podane nizej
#1) Pobrać tabele do prac manualnych i wybrać z niej linki do plików, których opracowanie nie zostało jeszcze rozpoczęte

!!!!  Dodać do kodu DP pętle, ktora wychwyci w kolumnie autor pipe'y', zeby wiedzialo, ze to dwóch autorów i nadało dwa viafy. W teh chwili kod nie ma tego warunku !!!!

darska, kryczytnym_okiem, poczytajmi, szelest_kartek - na poczatek dla tych stron (zwroc uwage na to, ze maja kilka kolumn z autorami np. Autor ksiazki). Instrukcja jest w pliku na Dysku: https://docs.google.com/document/d/19P9LOKV-SJSJkYVU34FscT39InGSZUy8wDDq4mgBllY/edit?tab=t.0#heading=h.ywhb11j28ynf 

1. Sprawdzenie kolumny AUtor pod kątem ilosci autorow:
    a. jesli 1 autor to w kolumnie VIAF autor 1 wpisuje VIAF
    b. jesli wiecej - obmyslic
    
2. Sprawdzenie kolumny Autor książki

4. Wysłanie tabeli uzupelnionej w viafu na dysk (aktualizacja tabeli )


3. Wstawić♥ funkcje DP do osobnego pliku i wywolywac za pomoc import

!!! Zastanowic sie nad wywolywaniem funkcji. Chyba najlepiej bedzie osobno dla kazdej tabeli, zeby moc korzystac z wielowatkowosci

"""
#%% import
import requests
from urllib.parse import urlencode
from fuzzywuzzy import fuzz
import re

from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from datetime import datetime
# from gspread.exceptions import WorksheetNotFound
import pandas as pd
import gspread as gs
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import regex as re
import numpy as np
import requests

from oauth2client.service_account import ServiceAccountCredentials
import time

from viaf_ulitmate import preprocess_text, extract_text_from_main_headings, check_viaf_with_fuzzy_match2



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
    df = get_as_dataframe(sheet.worksheet(worksheet), evaluate_formulas=True, dtype=str)
    return df



#%% Pozyskanie linkow do nieopracowanych tabel na podstawie tabeli Dokumentacja (prace manualne)

dokumentacja_prace_manualne_df = gsheet_to_df('1jCjEaopxsezprUiauuYkwQcG1cp40NqdhvxIzG5qUu8', 'dokumentacja')
'''Wyrzucenie pustych wierszy: '''
dokumentacja_prace_manualne_df = dokumentacja_prace_manualne_df.loc[dokumentacja_prace_manualne_df['NAZWA'].notna()]   

'''Uwzględnienie wierszy, które mają opracowane tabele manualne (dodany plik) '''
dokumentacja_prace_manualne_df = dokumentacja_prace_manualne_df.loc[dokumentacja_prace_manualne_df['LINK DO ARKUSZA'].notna()]

'''Wybranie tych wierszy, które mają STATUS PRAC "nie rozpoczęto" lub "False"'''
dokumentacja_prace_manualne_df_nie_rozpoczeto = dokumentacja_prace_manualne_df.loc[(dokumentacja_prace_manualne_df['STATUS PRAC'] == "nie rozpoczęto") | (dokumentacja_prace_manualne_df['STATUS PRAC'] == "False")]


'''Stworzenie listy linków do tabel, których opracowanie nie zostało jeszcze rozpoczęte''' #2024-11-26: 18 tabel
files_links = dokumentacja_prace_manualne_df_nie_rozpoczeto['LINK'].tolist()





#%% Functions


def list_of_authors_from_table(link):
    
    # link = 'https://docs.google.com/spreadsheets/d/1sjuv58WQwG3vfq7ikaRiUQxOVaF4tbu_XTgWhmamslU/edit?gid=652340147#gid=652340147'   #krytycznym_okiem
    # link = 'https://docs.google.com/spreadsheets/d/1x3B02W8PuIsq83HknVwpFnFRUrQQTZvzVLFmNndKJgw/edit?gid=652340147#gid=652340147'    #darska
    # link = 'https://docs.google.com/spreadsheets/d/1BoZyh226cX6t2nzoiLLiFB3RShNXjq4v-sGnAkQ8tJ8/edit?gid=652340147#gid=652340147'   #poczytajmi

    gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
    table_df = gsheet_to_df(gsheetId, 'Posts')
        
    list_of_unique_authors = list(set(table_df['Autor'].tolist())) + list(set(table_df['Autor książki'].tolist()))
    list_of_unique_authors = [re.sub(r"\([^)]*\)", "", e) for e in list_of_unique_authors if isinstance(e, str)]
    updated_authors = []
    
    for author in list_of_unique_authors:
        # author = list_of_unique_authors[10]
        if isinstance(author, str) and author:
            if re.search(r'\||\,|&|oraz|\/|\si\s ', author):  # Sprawdzamy, czy autor zawiera '|' lub ','
                authors = re.split(r'\||\,|&|oraz|\/|\si\s ', author)  # Dzielimy autorów
                updated_authors.extend([a.strip() for a in authors])  # Dodajemy do nowej listy
            elif len(author) > 40:   #Wyrzucenie błędnych danych (zamiast autora/autorów czasami pojawiają sie w tych polach długie teksty)
                continue
            else:
                updated_authors.append(author.strip())  # Dodajemy autora bez zmian
                
    updated_authors = list(set(updated_authors)) #Wyrzucenie duplikatów po splitowaniu
    updated_authors = [e for e in updated_authors if e] #Usunięcie pustych elementów
    return updated_authors


# Utworzenia slownika, w ktorym kluczami sa nazwy autorow, a wartoscia ich viafy     
def dictionary_of_authors_and_viafs(author):
    try:
        author_viaf = check_viaf_with_fuzzy_match2(author)
        dictionary_of_authors[author] = author_viaf[0][0]
    except TypeError: 
        dictionary_of_authors[author] = None
        print(author)
        
    return dictionary_of_authors




def update_viaf_columns(link, list_of_columns): #Pierwszy element listy to zawsze powinna być kolumna Autor/z autorami artykułu   
   
    gsheetId = re.search(r'(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
    table_df = gsheet_to_df(gsheetId, 'Posts')

    # list_of_columns = ['Autor', 'Autor książki']
    
    for i, column in enumerate(list_of_columns):
        # column = 'Autor książki'
        
        df_column_list = [re.sub(r"\([^)]*\)", "", e) if isinstance(e, str) else e for e in table_df[column].tolist()] 
        
        df_column_list = [re.split(r'\||\,|&|oraz|\/|\si\s ', e) if isinstance(e, str) else None for e in df_column_list]
        
        
        # df_column_list = [re.split(r'\||,', e) if isinstance(e, str) else None for e in table_df[column].tolist()]
        
        df_column_list_viafs = [[dictionary_of_authors.get(el.strip()) for el in e] if isinstance(e, list) else e for e in df_column_list]
        
        
        for il, e in enumerate(df_column_list_viafs):

            if e is not None and len(e) > 3:
                e = e[:2]
            elif e is not None and len(e) == 1:
                e.extend([None, None]) 
            elif e is not None and len(e) == 2:
                e.append(None) 
            elif e is not None and len(e) == 3: 
                pass
            else:
                df_column_list_viafs[il] = [None, None, None]
                
            
        
        if i == 0: 
            author_column_1 = [e[0] for e in df_column_list_viafs]
            table_df['VIAF autor 1'] = author_column_1
            
            author_column_2 = [e[1] for e in df_column_list_viafs]
            table_df['VIAF autor 2'] = author_column_2
            
            author_column_3 = [e[2] for e in df_column_list_viafs]
            table_df['VIAF autor 3'] = author_column_3
    
        elif i == 1:
            df_column_byt_1 = [e[0] for e in df_column_list_viafs]
            table_df['zewnętrzny identyfikator bytu 1'] = df_column_byt_1
            table_df['byt 1'] = table_df['zewnętrzny identyfikator bytu 1'].apply(lambda x: 'osoba' if x else None)
            
        elif i == 2: 
            df_column_byt_2 = [e[1] for e in df_column_list_viafs]
            table_df['zewnętrzny identyfikator bytu 2'] = df_column_byt_2
            table_df['byt 2'] = table_df['zewnętrzny identyfikator bytu 2'].apply(lambda x: 'osoba' if x else None)
            

    return table_df



            
        
#%% main

#%% 4 tabele wybrane do testowania modelu językowego:

# krytycznym_okiem = 'https://docs.google.com/spreadsheets/d/1sjuv58WQwG3vfq7ikaRiUQxOVaF4tbu_XTgWhmamslU/edit?gid=652340147#gid=652340147'
# darska = 'https://docs.google.com/spreadsheets/d/1x3B02W8PuIsq83HknVwpFnFRUrQQTZvzVLFmNndKJgw/edit?gid=652340147#gid=652340147'
# szelest_kartek = 'https://docs.google.com/spreadsheets/d/1P6A2gwFaglFh4r9Vk9k21-QSDrFpPYAVfDIzRqgw1D4/edit?gid=652340147#gid=652340147'


poczytajmi = 'https://docs.google.com/spreadsheets/d/1BoZyh226cX6t2nzoiLLiFB3RShNXjq4v-sGnAkQ8tJ8/edit?gid=652340147#gid=652340147'
    





poczytajmi = 'https://docs.google.com/spreadsheets/d/1BoZyh226cX6t2nzoiLLiFB3RShNXjq4v-sGnAkQ8tJ8/edit?gid=652340147#gid=652340147'

#!!! Plik ppoczytajmi to wyczyszczenia!
#!!! Tabele na dysku w kolumnie do PBL maja False zamiast tego takiego kwadracika




link = 'https://docs.google.com/spreadsheets/d/1BoZyh226cX6t2nzoiLLiFB3RShNXjq4v-sGnAkQ8tJ8/edit?gid=652340147#gid=652340147'


updated_authors = list_of_authors_from_table(link)                    
                    
dictionary_of_authors = {}
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_authors_and_viafs, updated_authors),total=len(updated_authors)))                 

    
df_darska = update_viaf_columns(link, ['Autor', 'Autor książki'])
df_krytycznym = update_viaf_columns(link, ['Autor', 'Autor książki'])

# df_poczytajmi = update_viaf_columns(link, ['Autor', 'Autor książki'])

df_szelest_kartek = update_viaf_columns(link, ['Autor', 'Autor książki'])

df_poczytajmi = update_viaf_columns(link, ['Autor', 'Autor książki'])



with pd.ExcelWriter(f"data\\viafowanie\\bernadetta_darska_2022-09-09.xlsx", engine='xlsxwriter') as writer:    
    df_darska.to_excel(writer, 'Posts', index=False)   

with pd.ExcelWriter(f"data\\viafowanie\\krytycznym_okiem_2024-10-04.xlsx", engine='xlsxwriter') as writer:    
    df_krytycznym.to_excel(writer, 'Posts', index=False)   
    
with pd.ExcelWriter(f"data\\viafowanie\\szelest_kartek_2022-09-01.xlsx", engine='xlsxwriter') as writer:    
    df_szelest_kartek.to_excel(writer, 'Posts', index=False)   


with pd.ExcelWriter(f"data\\viafowanie\\poczytajmi_blog_2024-10-18.xlsx", engine='xlsxwriter') as writer:    
    df_poczytajmi.to_excel(writer, 'Posts', index=False)   


#dopisz ręcznie viaf nasiriwiz : http://viaf.org/viaf/86147965902384082709







