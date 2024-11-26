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

# for index, row in tqdm(dokumentacja_prace_manualne_df_nie_rozpoczeto.iterrows()):  
#     try:
#         link = row['LINK']
     

def list_of_authors_from_table(link):
    
    # link = 'https://docs.google.com/spreadsheets/d/1sjuv58WQwG3vfq7ikaRiUQxOVaF4tbu_XTgWhmamslU/edit?gid=652340147#gid=652340147'   #krytycznym_okiem
    # link = 'https://docs.google.com/spreadsheets/d/1x3B02W8PuIsq83HknVwpFnFRUrQQTZvzVLFmNndKJgw/edit?gid=652340147#gid=652340147'    #darska

    gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
    table_df = gsheet_to_df(gsheetId, 'Posts')
        
    list_of_unique_authors = list(set(table_df['Autor'].tolist())) + list(set(table_df['Autor książki'].tolist()))
    updated_authors = []
    
    for author in list_of_unique_authors:
        # author = list_of_unique_authors[10]
        if isinstance(author, str) and author:
            if re.search(r'\||\,', author):  # Sprawdzamy, czy autor zawiera '|' lub ','
                authors = re.split(r'\||,', author)  # Dzielimy autorów
                updated_authors.extend([a.strip() for a in authors])  # Dodajemy do nowej listy
            else:
                updated_authors.append(author.strip())  # Dodajemy autora bez zmian


    updated_authors = list(set(updated_authors)) #Wyrzucenie duplikatów po splitowaniu
    return updated_authors


#Utworzenia slownika, w ktorym kluczami sa nazwy autorow, a wartoscia ich viafy     
def dictionary_of_authors_and_viafs(author):
    try:
        author_viaf = check_viaf_with_fuzzy_match2(author)
        dictionary_of_authors[author] = author_viaf[0][0]
    except TypeError: 
        dictionary_of_authors[author] = None
        print(author)
        
    return dictionary_of_authors


#Dokończ poniższą funkcje, wg instrukcji poniżej
#Update dataframe'u     
#Pamietaz zeby wczesniej zrobic split komorki, zeby wychwycilo miejsca gdzie jest wielu autorow i potem kazdy element ma ponumerowac i poszukac viafu. potem na podstwie numerow ma te viafy w odpowiednią kolumnę dac czyli np. Adam Kowalski, Jan Nowak -> [Adam Kowalski, Jan Nowak] -> (1, Adam Kowalski, viaf), (2, Jan Nowak, viaf) -> umiecic viafy w odpowiednich kolumnach wg kolejnosci 



def update_viaf_columns(link):  
             
    gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
    table_df = gsheet_to_df(gsheetId, 'Posts')



    # for k,v in dictionary_of_authors.items():
    #     if re.search(r'\||', k): 
    #         new_values_list = k.split('|')
    #     if re.search(r'\,', k): 
    #         new_values_list = k.split(',')
      

    #     if row['Autor'] in dictionary_of_authors.keys():
    #         table_df.at[index, 'VIAF autor 1'] = dictionary_of_authors.get(row['Autor'])
                        
            
    # for index, row in tqdm(table_df.iterrows()):    
            
            
            
            
    """
    Kolejny krok: Przesłanie zaktualizowanego DF na dysk google? Nadpisanie? Skonsultuj
    """
            
#%% main

link = 'https://docs.google.com/spreadsheets/d/1sjuv58WQwG3vfq7ikaRiUQxOVaF4tbu_XTgWhmamslU/edit?gid=652340147#gid=652340147'

updated_authors = list_of_authors_from_table(link)                    
                    
dictionary_of_authors = {}
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_authors_and_viafs, updated_authors),total=len(updated_authors)))                 

    
update_viaf_columns(link)






