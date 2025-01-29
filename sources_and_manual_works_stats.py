#%% 
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

#Raczej nie używać poniższej funkcji. Do późniejszej poprawki 
# def update_rekordy_pozyskane(df, max_retries=3, backoff_factor=2):
#     for index, row in tqdm(df.iterrows()):
#         link = row['LINK']
#         retries = 0
#         while retries < max_retries:
#             try:
#                 gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
#                 table_df = gsheet_to_df(gsheetId, 'Posts')
#                 all_extracted_records = table_df['Link'].dropna().shape[0]
#                 df.at[index, 'REKORDY POZYSKANE'] = all_extracted_records
#                 break  # Wyjdź z pętli, jeśli pobranie danych się powiodło
#             except requests.exceptions.RequestException as e:
#                 print(f"Błąd podczas pobierania danych dla wiersza {index}: {e}")
#                 retries += 1
#                 # Wykładniczy backoff: zwiększaj czas oczekiwania po każdej próbie
#                 wait_time = backoff_factor ** retries
#                 time.sleep(wait_time)
#             except Exception as e:
#                 df.at[index, 'REKORDY POZYSKANE'] = "Błąd"
#                 break
#         else:  # Jeśli pętla się wyczerpała
#             df.at[index, 'REKORDY POZYSKANE'] = "Błąd po wielu próbach"

#     return df


def update_rekordy_zaakceptowane(df, max_retries=3, backoff_factor=2):
    for index, row in tqdm(df.iterrows()):
        link = row['LINK']
        retries = 0
        while retries < max_retries:
            try:
                gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
                table_df = gsheet_to_df(gsheetId, 'Posts')
                filtered_df = table_df[(table_df['do PBL'] == 'True') & (table_df["Link"].notna())]
                accepted_records = len(filtered_df['Link'].tolist())
                df.at[index, 'REKORDY ZAAKCEPTOWANE'] = accepted_records
                break  # Wyjdź z pętli, jeśli pobranie danych się powiodło
            except requests.exceptions.RequestException as e:
                print(f"Błąd połączenia dla wiersza {index}: {e}")
                retries += 1
                # Wykładniczy backoff: zwiększaj czas oczekiwania po każdej próbie
                wait_time = backoff_factor ** retries
                time.sleep(wait_time)
            except Exception as e:
                df.at[index, 'REKORDY ZAAKCEPTOWANE'] = 0
                break
        else:  # Jeśli pętla się wyczerpała
            df.at[index, 'REKORDY ZAAKCEPTOWANE'] = "Błąd po wielu próbach"

    return df



def update_zakres_dat_w_zrodle(df):
    for index, row in tqdm(df.iterrows()):  
        try:
            link = row['LINK']
            if not pd.isna(link):  
                gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
                table_df = gsheet_to_df(gsheetId, 'Posts')

                dates_list = table_df['Data publikacji'].dropna().tolist()
                if dates_list != []:
                    first_date = min(dates_list)
                    last_date = max(dates_list)
                    df.at[index, 'ZAKRES DAT W ŹRÓDLE'] = f"{first_date}|{last_date}"  
                else:
                    df.at[index, 'ZAKRES DAT W ŹRÓDLE'] = None  
            else:
                df.at[index, 'ZAKRES DAT W ŹRÓDLE'] = "BRAK PRZYGOTOWANEJ TABELI"
        except (KeyError, TypeError):
            df.at[index, 'ZAKRES DAT W ŹRÓDLE'] = 'BRAK DATY PUBLIKACJI'  
        except:
            df.at[index, 'ZAKRES DAT W ŹRÓDLE'] = 'Do sprawdzenia'

    return df




def update_zakres_dat_oprac_rekordow(df):
    for index, row in tqdm(df.iterrows()):  
        try:
            link = row['LINK']
            if not pd.isna(link): 
                
                gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
                table_df = gsheet_to_df(gsheetId, 'Posts')

                filtered_df = table_df[(table_df['do PBL'] == 'True') & (table_df["Data publikacji"].notna())]
                years_list = filtered_df['Data publikacji'].tolist()

                if years_list != []:
                        first_year = min(years_list)
                        last_year = max(years_list)
                        df.at[index, 'ZAKRES LAT OPRAC. REKORDÓW'] = f"{first_year}|{last_year}"
                else:
                        df.at[index, 'ZAKRES LAT OPRAC. REKORDÓW'] = "OPRACOWANIE NIEROZPOCZĘTE"
            else:
                    df.at[index, 'ZAKRES LAT OPRAC. REKORDÓW'] = 'BRAK PRZYGOTOWANEJ TABELI'
                    
        except (KeyError, TypeError):
            df.at[index, 'ZAKRES LAT OPRAC. REKORDÓW'] = 'BRAK DATY PUBLIKACJI'  
        except:
            df.at[index, 'ZAKRES LAT OPRAC. REKORDÓW'] = 'Do sprawdzenia'

    return df


#%% main   

dokumentacja_prace_manualne_df = gsheet_to_df('1jCjEaopxsezprUiauuYkwQcG1cp40NqdhvxIzG5qUu8', 'dokumentacja')
dokumentacja_prace_manualne_df = dokumentacja_prace_manualne_df.loc[dokumentacja_prace_manualne_df['NAZWA'].notna()]   #Wyrzucenie pustych wierszy 
dokumentacja_prace_manualne_df = dokumentacja_prace_manualne_df.rename(columns={'NAZWA':'LINK DO STRONY'}) #Zmiana nazwy kolumny, żeby była taka sama jak w kolejnej tabeli (web scraping)
rekordy_pozyskane_wg_tabeli_oprac_manualne = dokumentacja_prace_manualne_df['REKORDY POZYSKANE'].fillna('0').astype("int64").sum()
# dokumentacja_prace_manualne_df['REKORDY ZAAKCEPTOWANE'].fillna('0').astype("int64").sum() #Te wartosci są praktycznie puste w tabeli

dokumentacja_web_scraping_df = gsheet_to_df('1-g_pgkvRIhBSKBENu_5HhRCMsHatv-eux3U_ERGHZG0', 'Raport')

merged_df = pd.merge(dokumentacja_prace_manualne_df, dokumentacja_web_scraping_df, on='LINK DO STRONY', how='left')
merged_df.columns   #Podgląd nazw kolumn, aby wybrać zbędne do wyrzucenia


final_df = merged_df.drop(columns=['CZY DO MANUALNYCH PRAC? (WG ZAŁĄCZNIKA DO PROJEKTU)', 'DZIEDZINA', 'uwagi do manualnych prac', 'data utworzenia',  'web scraping do poprawki', 'Unnamed: 14', 'KTO ROBI?', 'NAZWA PLIKU', 'PLIK XLSX', 'PLIK JSON',  'AKTYWNY?', 'DODATKI', 'UWAGI', 'LINK DO KODU', 'CZY DO MANUALNYCH PRAC?', 'Unnamed: 14', 'CZY DO OPRAC. MANUALNEGO? [UWZGLĘDNIONE ZMIANY]', 'REKORDY'])

final_df_only_available_sources = final_df.loc[(final_df['CZY POZYSKANO?'] != 'REZYGNACJA') & (final_df['CZY POZYSKANO?'] != 'NIEDOSTĘPNA')]

final_df_only_automatic = final_df_only_available_sources.loc[final_df['CZY DO PRAC MANUALNYCH (PO ZMIANACH)'] == 'NIE']



final_df_only_manual = final_df.loc[(final_df['CZY DO PRAC MANUALNYCH (PO ZMIANACH)'] == 'TAK')] 
final_df_only_manual = final_df_only_manual.loc[(final_df['CZY POZYSKANO?'] != 'REZYGNACJA')] 
final_df_only_manual = final_df_only_manual.reset_index(drop=True)

rekordy_pozyskane_tylko_manualne_na_podstawie_tabeli = final_df_only_manual['REKORDY POZYSKANE'].fillna('0').astype("int64").sum()

#%%DataFrame ze statystykami z dodatkowymi kolumnami (na potrzeby machine learningu)
#Uruchomienie funkcji do wzbogacenia danych dot. ilosci zeskrobanych i zaakceptowanych rekordow z poszczegolncyh serwisow + zakresu dat


#Funkcja update_rekordy_pozyskane zawsze zwraca inne wartosci niz sa w tabeli. W tabeli sa wartosci wpisywane z reki, wiec tez moga troche sie roznic w stosunku do rzeczywistosci
# update_rekordy_pozyskane(final_df_only_manual)
# #zamekczyta zwraca błąd
# #Checking
# if rekordy_pozyskane_tylko_manualne_na_podstawie_tabeli == final_df_only_manual['REKORDY POZYSKANE'].sum():
#     print("Zgadza się z tabelą z Dysku")
# else: 
#     x = final_df_only_manual['REKORDY POZYSKANE'].sum() - rekordy_pozyskane_tylko_manualne_na_podstawie_tabeli
#     print(f"Różnica między plikami to: {x}")
#  #Różnica miedzy plikami: -32359
    
        
    
update_rekordy_zaakceptowane(final_df_only_manual)  



extracted_not_assigned = final_df_only_manual.loc[(final_df_only_manual['REKORDY ZAAKCEPTOWANE'] == 0) & (final_df_only_manual['KTO'].isna()) & (final_df_only_manual['LINK DO ARKUSZA'].notna())] #Serwisy zeskrobane, ale jeszcze nieprzydzielone. Gotowe do przydzielenia
extracted_and_assigned_not_processed = final_df_only_manual.loc[(final_df_only_manual['REKORDY ZAAKCEPTOWANE'] == 0) & (final_df_only_manual['KTO'].notna()) & (final_df_only_manual['LINK DO ARKUSZA'].notna())] #0 zaakceptowanych, ale przydzielone. Do sprawdzenia czy jeszcze nie rozpoczęły te osoby prac, czy moze czegos nie policzylo


final_df_only_manual['REKORDY ZAAKCEPTOWANE'].sum()  
#2024-10-29: 17692
#2024-11-26: 18433
#2025-01-27: 20262


#Funkcje ponizej raczej dzialaja poprawnie
update_zakres_dat_w_zrodle(final_df_only_manual)    
update_zakres_dat_oprac_rekordow(final_df_only_manual)



#%% RAPORT ZBIORCZY (OGÓLNY i OPRAC. MANUALNE)
print('RAPORT z dnia ' + str(datetime.today().date()))
print("Liczba wszystkich serwisów: " + str(final_df.shape[0]) + ' (w tym niedostępne)')
print("Liczba wszystkich DOSTĘPNYCH serwisów: " + str(final_df_only_available_sources.shape[0]))
print("Liczba serwisów do oprac. manualnego: " + str(final_df_only_manual.shape[0]))
print("Liczba serwisów do oprac. automatycznego: " + str(final_df_only_automatic.shape[0]))
print('Zeskrobane serwisy: ' + str(final_df['CZY POZYSKANO?'].value_counts()['TAK']))                     
print('Zeskrobane serwisy (tylko do oprac. manualnego): ' + str(final_df_only_manual['CZY POZYSKANO?'].value_counts()['TAK']))

#Checking raport

if final_df_only_manual.shape[0] + final_df_only_automatic.shape[0] == final_df_only_available_sources.shape[0]:
    print("Wszystko się zgadza")
else: 
    print("UWAGA! BŁĄD! Suma źródeł do oprac. automatycznego i manualnego nie zgadza się z sumą wszystkich źródeł. Do sprawdzenia w tabelach")
    

print('PRACE MANUALNE')
print('Zakończono opracowanie: ' + str(final_df['STATUS PRAC'].value_counts()['zakończono']))        
print('Rozpoczęto opracowanie: ' + str(final_df['STATUS PRAC'].value_counts()['rozpoczęto']))
print('Przerwano opracowanie: ' + str(final_df['STATUS PRAC'].value_counts()['przerwano']))



#W poniższych sa błedy. do zbadania
print('Serwisy gotowe do przydzielenia (są tabelki): ' + str(final_df_only_manual.loc[(final_df_only_manual['czy_przekazano_do_manual'] == 'tak') & (final_df_only_manual['KTO'].isna())].shape[0]))

print('Rekordy zaakceptowane do PBL: ' + str(final_df_only_manual['REKORDY ZAAKCEPTOWANE'].sum())) #Trochę więcej. Sprawdzić ze statystykami


# Przygotowanie danych do DataFrame 
report_data = {
    'Opis': ['Data raportu', 'Liczba wszystkich serwisów', 'Liczba wszystkich DOSTĘPNYCH serwisów', 'Liczba serwisów do oprac. manualnego', 'Liczba serwisów do oprac. automatycznego', 'Zeskrobane serwisy','Zeskrobane serwisy (tylko do oprac. manualnego)', 'Zakończono opracowanie','Rozpoczęto opracowanie', 'Przerwano opracowanie', 'Serwisy gotowe do przydzielenia (są tabelki)', 'Rekordy zaakceptowane do PBL'],
    'Wartość': [datetime.today().date(), final_df.shape[0], final_df_only_available_sources.shape[0], final_df_only_manual.shape[0], final_df_only_automatic.shape[0], final_df['CZY POZYSKANO?'].value_counts()['TAK'], final_df_only_manual['CZY POZYSKANO?'].value_counts()['TAK'], final_df['STATUS PRAC'].value_counts()['zakończono'], final_df['STATUS PRAC'].value_counts()['rozpoczęto'], final_df['STATUS PRAC'].value_counts()['przerwano'], final_df_only_manual.loc[(final_df_only_manual['czy_przekazano_do_manual'] == 'tak') & (final_df_only_manual['KTO'].isna())].shape[0], final_df_only_manual['REKORDY ZAAKCEPTOWANE'].sum()]
}

# Tworzenie DataFrame
df = pd.DataFrame(report_data)

# Zapis do Excela
with pd.ExcelWriter(f"data\\raport.xlsx") as writer:
    df.to_excel(writer, sheet_name='Raport', index=False)












### KOMENTARZE
#Funkcja update_rekordy_pozyskane - raczej nie używac i pobierac zawartosci z tabeli, która bazuje na uzupełnieniach ręcznych. Przy korzystaniu z funkcji co rusz inne liczby zwraca + nie ma jak zliczyc serwisów, które są opracowane, ale jeszcze nie ma plików w tabeli do oprac. manualnego (np. Culture.pl - tylko json)
#ZAPISZ RAPORT w jakim formacie i wyslij do arkusza? Zastanowić się
#Napisać testy jednostkowe dla tego pliku 






#Wyrzucenie zbędnych dla uczenia maszynowego kolumn:
machine_learning_df = final_df_only_manual.drop(columns=['CZY DO PRAC MANUALNYCH (PO ZMIANACH)', 'czy_przekazano_do_manual', 'CZY POZYSKANO?', 'OSOBA OPRACOWUJĄCA', 'STATUS PRAC'])
machine_learning_df = machine_learning_df.rename(columns={'LINK DO ARKUSZA': 'NAZWA PLIKU', 'LINK':'LINK DO PLIKU'})

#Utworzyć z tego plik xlsx
#Na końcu wysłać tabele na dysk do osobnego arkusza


with pd.ExcelWriter(f"data\iPBL_manual_works_stats_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    machine_learning_df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()   




















