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
from gspread.exceptions import WorksheetNotFound
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

# from viaf_ulitmate import preprocess_text, extract_text_from_main_headings, check_viaf_with_fuzzy_match2 (Darka)
from viaf_ulitmate import normalize_name, get_best_viaf_link  #Julius AI



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


dokumentacja_prace_manualne_df_nie_rozpoczeto_brak_viafowania = dokumentacja_prace_manualne_df_nie_rozpoczeto.loc[dokumentacja_prace_manualne_df_nie_rozpoczeto['uwagi'] != 'viafowanie']

'''Stworzenie listy linków do tabel, których opracowanie nie zostało jeszcze rozpoczęte''' #2024-11-26: 18 tabel
files_links = dokumentacja_prace_manualne_df_nie_rozpoczeto_brak_viafowania['LINK'].tolist()





#%% Functions


def list_of_authors_from_table(link):

    gsheetId = re.search(r'(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
    table_df = gsheet_to_df(gsheetId, 'Posts')
    
    try:    
        list_of_unique_authors = list(set(table_df['Autor'].tolist())) + list(set(table_df['Autor książki'].tolist()))
        list_of_unique_authors = [re.sub(r"\([^)]*\)", "", e) for e in list_of_unique_authors if isinstance(e, str)]
    except KeyError:    #Gdy nie ma kolumny 'Autor książki'
        list_of_unique_authors = set(table_df['Autor'].tolist())
        
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
    updated_authors = [re.sub(r'([\p{L}]*:)(.*)', r'\2', e).strip() if re.search(r'[\p{L}]*:', e) else e for e in updated_authors]    #dla zamek_czyta
    updated_authors = [re.sub(r'^(.*?)(\s*[\u2014].*)?$', r'\1', e).strip() if re.search(r'\s[\u2014]\s', e) else e for e in updated_authors]    #dla musisieukazac

    
    updated_authors = [re.sub(r'(\p{L}+)(\p{Lu})', r'\1 \2', e) for e in updated_authors]    #dla poczytajdziecku
    
    return updated_authors


# Utworzenia slownika, w ktorym kluczami sa nazwy autorow, a wartoscia ich viafy     #2025-02-18 dodałam inna funcje zamiast check_viaf_with_fuzzy_match2
def dictionary_of_authors_and_viafs(author):
    # author = "Adam Mickiewicz"
    try:
        # author = 'Piotr Gaszczyński'
        author_viaf = get_best_viaf_link(author)
        dictionary_of_authors[author] = author_viaf
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

#%% Wykomentowane zmienne to linki do już uzupełnionych o viafy tabel

# Done: 
# krytycznym_okiem = 'https://docs.google.com/spreadsheets/d/1sjuv58WQwG3vfq7ikaRiUQxOVaF4tbu_XTgWhmamslU/edit?gid=652340147#gid=652340147'
# darska = 'https://docs.google.com/spreadsheets/d/1x3B02W8PuIsq83HknVwpFnFRUrQQTZvzVLFmNndKJgw/edit?gid=652340147#gid=652340147'
# szelest_kartek = 'https://docs.google.com/spreadsheets/d/1P6A2gwFaglFh4r9Vk9k21-QSDrFpPYAVfDIzRqgw1D4/edit?gid=652340147#gid=652340147'
# poczytajmi = 'https://docs.google.com/spreadsheets/d/1BoZyh226cX6t2nzoiLLiFB3RShNXjq4v-sGnAkQ8tJ8/edit?gid=652340147#gid=652340147'   
# zamek_czyta = 'https://docs.google.com/spreadsheets/d/1YbfqA_1EIx7KOGJD3PelK-TBLavg7N_oGoja1tv__Zs/edit?gid=652340147#gid=652340147'
# god_save_the_book = 'https://docs.google.com/spreadsheets/d/1pNQos-vdEeTmz-kN3wQu2h9dDHUxS0Qp8-oy1ACJqRg/edit?gid=652340147#gid=652340147'
# giedrys = 'https://docs.google.com/spreadsheets/d/1pIqWc1d5xAjgcIcNY_Hzj6ffIutD8tKnEguczB9mtuA/edit?gid=652340147#gid=652340147'
# kulturaliberalna = 'https://docs.google.com/spreadsheets/d/1qxVX3LVPnDr5fUliLzCIyu-QruZu3-_aIIlb-HGVHX0/edit?gid=652340147#gid=652340147'
# o_poezji = 'https://docs.google.com/spreadsheets/d/1jn7Ev88NR6NSbCGNacqvl2LfDdVYqanI-rb7CRgbEE0/edit?gid=652340147#gid=652340147'
# poledwumiesiecznik = 'https://docs.google.com/spreadsheets/d/1zaNhC9b186MO7935fjVceAUW8aCvUfwPa742Rh-9EGI/edit?gid=652340147#gid=652340147'
# stoner_polski = 'https://docs.google.com/spreadsheets/d/1Qkjeq5CxY-CJQ7fiAIvRGLzVUX0wDjs4mpYHr-QnHlQ/edit?gid=652340147#gid=652340147'
# pelna_sala = 'https://docs.google.com/spreadsheets/d/1_TVR3TJEvPgN6I3_wmqlqVzq9FtUAYtcQWIYAl0MAW4/edit?gid=652340147#gid=652340147'
# poczytaj_dziecku = 'https://docs.google.com/spreadsheets/d/1L0hm5yZPYfI5vxtTfRX2y4yEMl-ARyL2_nmplo88uw8/edit?gid=652340147#gid=652340147'
# musi_sie_ukazac = 'https://docs.google.com/spreadsheets/d/1hnQvR-GJ1BmqMU-QXhkaXeVSMiZFsSdv0HM8QM0NPY8/edit?gid=652340147#gid=652340147'  Zrobione, ale jakies malo viafow i raczej blednie przypisane, wiec nie uzupelnilam tabeli na dysku
# moja_przestrzen_kultury = 'https://docs.google.com/spreadsheets/d/1aIpcIbhcTEwRgmCmAAKgrNJ4IjLEEDUyhNcJeE3R-oo/edit?gid=652340147#gid=652340147'
#rue_bout_du_monde = 'https://docs.google.com/spreadsheets/d/1bg5uSkpNpWK--8tjK3w0vG-vt6q7Ul09n8imGRTNJUU/edit#gid=652340147' - kod sobie nie poradził. Nie zrównało Radka Kobierskiego z Radosławem Kobierskim
#w_cieniu_skrzydel = 'https://docs.google.com/spreadsheets/d/10OTdaRwg7y1oxvJU9FFzV-J5MVbgYF3h47BC9LBv9PI'
#kontent = 'https://docs.google.com/spreadsheets/d/1OYnWnxUCEZyOp4JNdR06WMWb-08fBJQfzp-MUX3A2CA/edit?gid=652340147#gid=652340147'
# helikopter = 'https://docs.google.com/spreadsheets/d/1zCd2Q1orjawLtE7pvNTHFph2FvCGzb0ooWclCCV8RMI/edit?gid=652340147#gid=652340147'
# ksiazkinaostro = 'https://docs.google.com/spreadsheets/d/1Xuw74NHb0MzmxHmdXAuOPXSxq0T_3NZW2khfr0xsaPQ/edit?gid=652340147#gid=652340147'


#NIE ROBIĆ: 
#rozdzielczosc_chleba - brak autorów

link = 'https://docs.google.com/spreadsheets/d/1WTM6ELxoMeSGswNkiEYMLAR0tPxYKVT7hWsYMhkCqbI/edit?gid=652340147#gid=652340147' #ZROBIC PO NAPRAWIENIU KODU DO VIAFU
# link = 'https://docs.google.com/spreadsheets/d/1Xuw74NHb0MzmxHmdXAuOPXSxq0T_3NZW2khfr0xsaPQ/edit?gid=652340147#gid=652340147' #ZROBIC PO NAPRAWIENIU KODU DO VIAFU


updated_authors = list_of_authors_from_table(link)                    
                    
dictionary_of_authors = {}
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_authors_and_viafs, updated_authors),total=len(updated_authors)))                 



#%% Tworzenie dataframe. Pamiętać, żeby wpisać nazwy kolumn z df    
# update_viaf_columns(link, ['Autor', 'Autor książki']) - zawsze uwzględniac nazwy kolumn!


df = update_viaf_columns(link, ['Autor'])

with pd.ExcelWriter(r"data\\viafowanie\\helikopter_2024-12-16.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   

   











