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



dokumentacja_prace_manualne_df = gsheet_to_df('1jCjEaopxsezprUiauuYkwQcG1cp40NqdhvxIzG5qUu8', 'dokumentacja')
dokumentacja_prace_manualne_df = dokumentacja_prace_manualne_df.loc[dokumentacja_prace_manualne_df['NAZWA'].notna()]    #Wyrzucenie pustych wierszy 
dokumentacja_prace_manualne_df = dokumentacja_prace_manualne_df.rename(columns={'NAZWA':'LINK DO STRONY'}) 


dokumentacja_web_scraping_df = gsheet_to_df('1-g_pgkvRIhBSKBENu_5HhRCMsHatv-eux3U_ERGHZG0', 'Raport')


merged_df = pd.merge(dokumentacja_prace_manualne_df, dokumentacja_web_scraping_df, on='LINK DO STRONY', how='left')
merged_df.columns

final_df = merged_df.drop(columns=['CZY DO MANUALNYCH PRAC? (WG ZAŁĄCZNIKA DO PROJEKTU)', 'DZIEDZINA', 'uwagi do manualnych prac', 'OSOBA OPRACOWUJĄCA', 'data utworzenia',  'web scraping do poprawki', 'Unnamed: 13', 'KTO ROBI?', 'NAZWA PLIKU', 'PLIK XLSX', 'PLIK JSON',  'AKTYWNY?', 'DODATKI', 'UWAGI', 'LINK DO KODU', 'CZY DO MANUALNYCH PRAC?', 'Unnamed: 14', 'CZY DO OPRAC. MANUALNEGO? [UWZGLĘDNIONE ZMIANY]', 'REKORDY'])



final_df_only_manual = final_df.loc[(final_df['CZY DO PRAC MANUALNYCH (PO ZMIANACH)'] == 'TAK')] 
final_df_only_manual = final_df_only_manual.loc[(final_df['CZY POZYSKANO?'] != 'REZYGNACJA')] 

# next(final_df_only_manual.iterrows())


final_df_only_manual['CZY POZYSKANO?'].value_counts() #66 zeskrobanych, 25 do zeskrobania (sposród tych do prac manualnych)


for index, row in tqdm(final_df_only_manual.iterrows()):
    
    link = row['LINK'] 
    try:
        gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
        table_df = gsheet_to_df(gsheetId, 'Posts')
        true_counts = table_df['do PBL'].value_counts()['True']
        row['REKORDY ZAAKCEPTOWANE'] = true_counts
    except (KeyError, TypeError):
        row['REKORDY ZAAKCEPTOWANE'] = None
    

#Po kilku wierszach wyskoczył JSONDecodeError: Expecting value (Po culture.pl) Aict problem: niby link usuniety...














































