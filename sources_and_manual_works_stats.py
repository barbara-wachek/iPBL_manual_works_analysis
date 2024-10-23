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


def update_rekordy_zaakceptowane(df):
    for index, row in tqdm(df.iterrows()):  
        try:
            link = row['LINK']
            if not pd.isna(link):  # Check if 'LINK' is not null
                gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
                table_df = gsheet_to_df(gsheetId, 'Posts')
                all_extracted_records = table_df['Link'].dropna().shape[0]
                filtered_df = table_df[(table_df['do PBL'] == 'True') & (table_df["Data publikacji"].notna())]
                accepted_records = len(filtered_df['Link'].tolist())
                
                df.at[index, 'REKORDY ZAAKCEPTOWANE'] = accepted_records
                df.at[index, 'REKORDY POZYSKANE'] = all_extracted_records
                
            else:
                df.at[index, 'REKORDY ZAAKCEPTOWANE'] = None  
                df.at[index, 'REKORDY POZYSKANE'] = None
        except (KeyError, TypeError):
            df.at[index, 'REKORDY ZAAKCEPTOWANE'] = None  
            df.at[index, 'REKORDY POZYSKANE'] = None
        except:
            df.at[index, 'REKORDY ZAAKCEPTOWANE'] = "DO UZUPEŁNIENIA RĘCZNIE?" 
            df.at[index, 'REKORDY POZYSKANE'] = "DO UZUPEŁNIENIA RĘCZNIE?" 

    return df 



def update_zakres_lat_w_zrodle(df):
    """Updates the 'TEST' column in the DataFrame based on years extracted from 'Data publikacji'.
    Args:
        df (pd.DataFrame): The DataFrame containing the 'LINK' and 'TEST' columns.
    Returns:
        pd.DataFrame: The modified DataFrame with the updated 'TEST' column.
    """

    for index, row in tqdm(df.iterrows()):  
        try:
            link = row['LINK']
            if not pd.isna(link):  # Check if 'LINK' is not null
                gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
                table_df = gsheet_to_df(gsheetId, 'Posts')

                # Extract years using a more robust approach (consider edge cases)
                years_list = table_df['Data publikacji'].dropna().tolist()
                if years_list:
                    first_year = min(years_list)
                    last_year = max(years_list)
                    df.at[index, 'ZAKRES LAT W ŹRÓDLE'] = f"{first_year}|{last_year}"  # Efficient formatting
                else:
                    df.at[index, 'ZAKRES LAT W ŹRÓDLE'] = None  # Set 'TEST' to None if no years found
        except (KeyError, TypeError):
            df.at[index, 'ZAKRES LAT W ŹRÓDLE'] = None  # Handle errors gracefully
        except:
            df.at[index, 'ZAKRES LAT W ŹRÓDLE'] = 'Do uzupełnienia ręcznie'

    return df



def update_zakres_lat_oprac_rekordow(df):
    """Updates the 'TEST' column in the DataFrame based on years extracted from 'Data publikacji'.
    Args:
        df (pd.DataFrame): The DataFrame containing the 'LINK' and 'TEST' columns.
    Returns:
        pd.DataFrame: The modified DataFrame with the updated 'TEST' column.
    """

    for index, row in tqdm(df.iterrows()):  
        try:
            link = row['LINK']
            if not pd.isna(link):  # Check if 'LINK' is not null
                gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
                table_df = gsheet_to_df(gsheetId, 'Posts')

                filtered_df = table_df[(table_df['do PBL'] == 'True') & (table_df["Data publikacji"].notna())]
                years_list = filtered_df['Data publikacji'].tolist()

                if years_list:
                        first_year = min(years_list)
                        last_year = max(years_list)
                        df.at[index, 'ZAKRES LAT OPRAC. REKORDÓW'] = f"{first_year}|{last_year}"
                else:
                        df.at[index, 'ZAKRES LAT OPRAC. REKORDÓW'] = None
            else:
                    df.at[index, 'ZAKRES LAT OPRAC. REKORDÓW'] = None
                    
        except (KeyError, TypeError):
            df.at[index, 'ZAKRES LAT OPRAC. REKORDÓW'] = None  
        except:
            df.at[index, 'ZAKRES LAT OPRAC. REKORDÓW'] = 'Do sprawdzenia'

    return df


#%% main   

dokumentacja_prace_manualne_df = gsheet_to_df('1jCjEaopxsezprUiauuYkwQcG1cp40NqdhvxIzG5qUu8', 'dokumentacja')
dokumentacja_prace_manualne_df = dokumentacja_prace_manualne_df.loc[dokumentacja_prace_manualne_df['NAZWA'].notna()]   #Wyrzucenie pustych wierszy 
dokumentacja_prace_manualne_df = dokumentacja_prace_manualne_df.rename(columns={'NAZWA':'LINK DO STRONY'}) #Zmiana nazwy kolumny, żeby była taka sama jak w kolejnej tabeli (web scraping)

dokumentacja_web_scraping_df = gsheet_to_df('1-g_pgkvRIhBSKBENu_5HhRCMsHatv-eux3U_ERGHZG0', 'Raport')

merged_df = pd.merge(dokumentacja_prace_manualne_df, dokumentacja_web_scraping_df, on='LINK DO STRONY', how='left')
merged_df.columns   #Podgląd nazw kolumn, aby wybrać zbędne do wyrzucenia


final_df = merged_df.drop(columns=['CZY DO MANUALNYCH PRAC? (WG ZAŁĄCZNIKA DO PROJEKTU)', 'DZIEDZINA', 'uwagi do manualnych prac', 'OSOBA OPRACOWUJĄCA', 'data utworzenia',  'web scraping do poprawki', 'Unnamed: 13', 'KTO ROBI?', 'NAZWA PLIKU', 'PLIK XLSX', 'PLIK JSON',  'AKTYWNY?', 'DODATKI', 'UWAGI', 'LINK DO KODU', 'CZY DO MANUALNYCH PRAC?', 'Unnamed: 14', 'CZY DO OPRAC. MANUALNEGO? [UWZGLĘDNIONE ZMIANY]', 'REKORDY'])



#Poniżej stworzenie DF tylko z serwisami do opracowania manualnego + wyrzucenie 1 serwisu, z ktorego zrezygnowano (2024-10-23 - 91 serwisów)
final_df_only_manual = final_df.loc[(final_df['CZY DO PRAC MANUALNYCH (PO ZMIANACH)'] == 'TAK')] 
final_df_only_manual = final_df_only_manual.loc[(final_df['CZY POZYSKANO?'] != 'REZYGNACJA')] 
final_df_only_manual = final_df_only_manual.reset_index(drop=True)

#RAPORT DOT. ORACOWANIA MANUALNEGO
print('RAPORT DOT. SERWISÓW DO OPRAC. MANUALNEGO')
print("Liczba serwisów: " + str(final_df_only_manual.shape[0]))
print('Zeskrobane: ' + str(final_df_only_manual['CZY POZYSKANO?'].value_counts()['TAK']))
print('Zakończono opracowanie: ' + str(final_df_only_manual['STATUS PRAC'].value_counts()['zakończono']))
print('Rozpoczęto opracowanie: ' + str(final_df_only_manual['STATUS PRAC'].value_counts()['rozpoczęto']))
print('Przerwano opracowanie: ' + str(final_df_only_manual['STATUS PRAC'].value_counts()['przerwano']))


# #Dodanie dwóch kolumn: 
# final_df_only_manual[['ZAKRES LAT W ŹRÓDLE', 'ZAKRES LAT OPRAC. REKORDÓW']] = np.nan

#Uruchomienie funkcji do wzbogacenia danych dot. ilosci zeskrobanych i zaakceptowanych rekordow z poszczegolncyh serwisow + zakresu dat
update_rekordy_zaakceptowane(final_df_only_manual)  #Zlicza rekordy zaakceptowane w trakcie prac manualnych oraz wszystkie zeskrobane
update_zakres_lat_w_zrodle(final_df_only_manual)    
update_zakres_lat_oprac_rekordow(final_df_only_manual)


#Wyrzucenie zbędnych dla uczenia maszynowego kolumn:
machine_learning_df = final_df_only_manual.drop(columns=['CZY DO PRAC MANUALNYCH (PO ZMIANACH)', 'czy_przekazano_do_manual', 'CZY POZYSKANO?'])
machine_learning_df = machine_learning_df.drop(columns=['STATUS PRAC'])
machine_learning_df = machine_learning_df.rename(columns={'LINK DO ARKUSZA': 'NAZWA PLIKU', 'LINK':'LINK DO PLIKU'})

#Utworzyć z tego plik xlsx
#Na końcu wysłać tabele na dysk do osobnego arkusza


with pd.ExcelWriter(f"data\iPBL_manual_works_stats_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    machine_learning_df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()   



#%% Przesłanie tabeli DOPRACUJ

# gauth = GoogleAuth()           
# drive = GoogleDrive(gauth)   
      
# upload_file_list = [f"krytycznym_okiem_{datetime.today().date()}.xlsx", f'krytycznym_okiem_{datetime.today().date()}.json']
# for upload_file in upload_file_list:
# 	gfile = drive.CreateFile({'parents': [{'id': '1jCjEaopxsezprUiauuYkwQcG1cp40NqdhvxIzG5qUu8'}]})  
# 	gfile.SetContentFile(upload_file)
# # 	gfile.Upload()        



# gauth = GoogleAuth()           
# drive = GoogleDrive(gauth)   
      
# upload_file= f"iPBL_manual_works_stats_{datetime.today().date()}.xlsx"

# gfile = drive.CreateFile({'parents': [{'id': '1ZrLyjsA6Q-k78M8gpuK5EB2NXCk56zA0'}]})  
# gfile.SetContentFile(upload_file)
# gfile.Upload()         


















 #DO USUNIECIA
# for index, row in tqdm(final_df_only_manual.iterrows()):
#     link = row['LINK'] 
#     try:
#         # link = 'https://docs.google.com/spreadsheets/d/1H44U6tl1OWl5Y2ScHyp__OlUR7vab-zbyBkqAQKZryE'
#         gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
#         table_df = gsheet_to_df(gsheetId, 'Posts')
#         first_year = re.search(r'\d\d\d\d', table_df['Data publikacji'][0]).group()
#         last_row_number = table_df['Data publikacji'].dropna().shape[0]
#         last_year = re.search(r'\d\d\d\d', table_df.iloc[last_row_number-1,1]).group()
        
#         # final_df_only_manual.loc[index, row['TEST']] = last_year
        
#         # if last_year < first_year:
#         #     row['TEST'] = str(last_year) + "-" + str(first_year)
#         # else:
#         #     row['TEST'] = str(first_year) + "-" + str(last_year)
            
#     except (KeyError, TypeError):
#         row['TEST'] = None


# def update_liczba_zapisow(df):

#     for index, row in df.iterrows():  
#         try:
#             link = row['LINK']
#             if not pd.isna(link):  # Check if 'LINK' is not null
#                 gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
#                 table_df = gsheet_to_df(gsheetId, 'Posts')

#                 all_extracted_records = table_df['Link'].dropna().shape[0]
#                 filtered_df = table_df[(table_df['do PBL'] == 'True') & (table_df["Data publikacji"].notna())]
#                 accepted_records = len(filtered_df['Link'].tolist())
                
#                 df.at[index, 'LICZBA ZESKROBANYCH REKORDÓW'] = all_extracted_records
#                 df.at[index, 'LICZBA OPRACOWANYCH REKORDÓW'] = accepted_records
                
#             else:
#                 df.at[index, 'LICZBA ZESKROBANYCH REKORDÓW'] = None  
#                 df.at[index, 'LICZBA OPRACOWANYCH REKORDÓW'] = None
#         except (KeyError, TypeError):
#             df.at[index, 'LICZBA ZESKROBANYCH REKORDÓW'] = None  
#             df.at[index, 'LICZBA OPRACOWANYCH REKORDÓW'] = None
#         except:
#             df.at[index, 'LICZBA ZESKROBANYCH REKORDÓW'] = "DO UZUPEŁNIENIA RĘCZNIE?" 
#             df.at[index, 'LICZBA OPRACOWANYCH REKORDÓW'] = "DO UZUPEŁNIENIA RĘCZNIE?" 

#     return df 


# def update_liczba_zapisow(df):

#     for index, row in df.iterrows():  
#         try:
#             link = row['LINK']
#             if not pd.isna(link):  # Check if 'LINK' is not null
#                 gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
#                 table_df = gsheet_to_df(gsheetId, 'Posts')

#                 all_extracted_records = table_df['Link'].dropna().shape[0]
#                 filtered_df = table_df[(table_df['do PBL'] == 'True') & (table_df["Data publikacji"].notna())]
#                 accepted_records = len(filtered_df['Link'].tolist())
                
#                 df.at[index, 'LICZBA ZESKROBANYCH REKORDÓW'] = all_extracted_records
#                 df.at[index, 'LICZBA OPRACOWANYCH REKORDÓW'] = accepted_records
                
#             else:
#                 df.at[index, 'LICZBA ZESKROBANYCH REKORDÓW'] = None  
#                 df.at[index, 'LICZBA OPRACOWANYCH REKORDÓW'] = None
#         except (KeyError, TypeError):
#             df.at[index, 'LICZBA ZESKROBANYCH REKORDÓW'] = None  
#             df.at[index, 'LICZBA OPRACOWANYCH REKORDÓW'] = None
#         except:
#             df.at[index, 'LICZBA ZESKROBANYCH REKORDÓW'] = "DO UZUPEŁNIENIA RĘCZNIE?" 
#             df.at[index, 'LICZBA OPRACOWANYCH REKORDÓW'] = "DO UZUPEŁNIENIA RĘCZNIE?" 

#     return df 





