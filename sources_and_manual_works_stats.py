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


def update_rekordy_pozyskane(df):
    for index, row in tqdm(df.iterrows()):  
        try:
            link = row['LINK']
            # if not pd.isna(link):  # Check if 'LINK' is not null
                # link = 'https://docs.google.com/spreadsheets/d/1zOxDVFLvk2ovJEyyKp-Zpw6Vqdqr3WG-nueOLy5cVs4/edit#gid=652340147'
                # link = None
                # link = 'https://docs.google.com/spreadsheets/d/1ynR57xAatPRKayLn1ZhQ6s3uP_lXSTria0GtlkuH6TY/edit?gid=652340147#gid=652340147'
            gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
            table_df = gsheet_to_df(gsheetId, 'Posts')
            all_extracted_records = table_df['Link'].dropna().shape[0]
            df.at[index, 'REKORDY POZYSKANE'] = all_extracted_records
          
            # else:
            #     df.at[index, 'REKORDY POZYSKANE'] = 'None'
       
        except (KeyError, TypeError):  
            df.at[index, 'REKORDY POZYSKANE'] = None
        except:
            df.at[index, 'REKORDY POZYSKANE'] = "JAKIŚ BŁĄD" 

    return df 


def update_rekordy_zaakceptowane(df):
    for index, row in tqdm(df.iterrows()):  
        try:
            link = row['LINK']
            # if not pd.isna(link):  # Check if 'LINK' is not null
            # link = 'https://docs.google.com/spreadsheets/d/1zOxDVFLvk2ovJEyyKp-Zpw6Vqdqr3WG-nueOLy5cVs4/edit#gid=652340147'
            # link = 'https://docs.google.com/spreadsheets/d/1L0hm5yZPYfI5vxtTfRX2y4yEMl-ARyL2_nmplo88uw8/edit?gid=652340147#gid=652340147'
            # # link = 'https://docs.google.com/spreadsheets/d/1W4jGEOW7YfbJZw9qTFHb65vxMbrlyb02XwJnpvx8BLs/edit#gid=652340147'
            # link = 'https://docs.google.com/spreadsheets/d/1sjuv58WQwG3vfq7ikaRiUQxOVaF4tbu_XTgWhmamslU/edit?gid=652340147#gid=652340147' #krytyczynym
            # link = 'https://docs.google.com/spreadsheets/d/1L0hm5yZPYfI5vxtTfRX2y4yEMl-ARyL2_nmplo88uw8/edit?gid=652340147#gid=652340147' #poczytajdziecku
            # link = 'https://docs.google.com/spreadsheets/d/1rIN84s4N_5cQPcxIa343Smls8HH9tvObE4wT3B24iSw/edit#gid=652340147'
            gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
            table_df = gsheet_to_df(gsheetId, 'Posts')
           
            filtered_df = table_df[(table_df['do PBL'] == 'True') & (table_df["Link"].notna())]
            accepted_records = len(filtered_df['Link'].tolist())
            df.at[index, 'REKORDY ZAAKCEPTOWANE'] = accepted_records
                    
            # else:
            #     df.at[index, 'REKORDY ZAAKCEPTOWANE'] = 'None'  
       
        except (KeyError, TypeError):
            df.at[index, 'REKORDY ZAAKCEPTOWANE'] = 'None'
        except:
            df.at[index, 'REKORDY ZAAKCEPTOWANE'] = "DO UZUPEŁNIENIA RĘCZNIE?" 

    return df 




def update_zakres_lat_w_zrodle(df):
    for index, row in tqdm(df.iterrows()):  
        try:
            link = row['LINK']
            if not pd.isna(link):  
                # link = 'https://docs.google.com/spreadsheets/d/1T6JHJxlZJtAhUpYNS37CTbRmRN5Q4-_YYD6GXa2MADc'
                gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
                table_df = gsheet_to_df(gsheetId, 'Posts')

                years_list = table_df['Data publikacji'].dropna().tolist()
                if years_list != []:
                    first_year = min(years_list)
                    last_year = max(years_list)
                    df.at[index, 'ZAKRES LAT W ŹRÓDLE'] = f"{first_year}|{last_year}"  
                else:
                    df.at[index, 'ZAKRES LAT W ŹRÓDLE'] = None  
        except (KeyError, TypeError):
            df.at[index, 'ZAKRES LAT W ŹRÓDLE'] = 'BRAK DATY PUBLIKACJI'  
        except:
            df.at[index, 'ZAKRES LAT W ŹRÓDLE'] = 'Do sprawdzenia'

    return df



def update_zakres_lat_oprac_rekordow(df):
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


dokumentacja_web_scraping_df = gsheet_to_df('1-g_pgkvRIhBSKBENu_5HhRCMsHatv-eux3U_ERGHZG0', 'Raport')

merged_df = pd.merge(dokumentacja_prace_manualne_df, dokumentacja_web_scraping_df, on='LINK DO STRONY', how='left')
merged_df.columns   #Podgląd nazw kolumn, aby wybrać zbędne do wyrzucenia


final_df = merged_df.drop(columns=['CZY DO MANUALNYCH PRAC? (WG ZAŁĄCZNIKA DO PROJEKTU)', 'DZIEDZINA', 'uwagi do manualnych prac', 'data utworzenia',  'web scraping do poprawki', 'Unnamed: 13', 'KTO ROBI?', 'NAZWA PLIKU', 'PLIK XLSX', 'PLIK JSON',  'AKTYWNY?', 'DODATKI', 'UWAGI', 'LINK DO KODU', 'CZY DO MANUALNYCH PRAC?', 'Unnamed: 14', 'CZY DO OPRAC. MANUALNEGO? [UWZGLĘDNIONE ZMIANY]', 'REKORDY'])

final_df_only_available_sources = final_df.loc[(final_df['CZY POZYSKANO?'] != 'REZYGNACJA') & (final_df['CZY POZYSKANO?'] != 'NIEDOSTĘPNA')]



final_df_only_automatic = final_df_only_available_sources.loc[final_df['CZY DO PRAC MANUALNYCH (PO ZMIANACH)'] == 'NIE']

final_df_only_manual = final_df.loc[(final_df['CZY DO PRAC MANUALNYCH (PO ZMIANACH)'] == 'TAK')] 
final_df_only_manual = final_df_only_manual.loc[(final_df['CZY POZYSKANO?'] != 'REZYGNACJA')] 
final_df_only_manual = final_df_only_manual.reset_index(drop=True)



#%%DataFrame ze statystykami z dodatkowymi kolumnami (na potrzeby machine learningu)
#Uruchomienie funkcji do wzbogacenia danych dot. ilosci zeskrobanych i zaakceptowanych rekordow z poszczegolncyh serwisow + zakresu dat
update_rekordy_pozyskane(final_df_only_manual)
update_rekordy_zaakceptowane(final_df_only_manual)  #Nie liczy xksięgarni! link = 'https://docs.google.com/spreadsheets/d/1rIN84s4N_5cQPcxIa343Smls8HH9tvObE4wT3B24iSw/edit#gid=652340147
update_zakres_lat_w_zrodle(final_df_only_manual)    
update_zakres_lat_oprac_rekordow(final_df_only_manual)



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
print('Gotowe do przydzielenia: ' + str(final_df_only_manual.loc[(final_df_only_manual['czy_przekazano_do_manual'] == 'tak') & (final_df_only_manual['KTO'].isna())].shape[0]))

print('Opracowane rekordy: ' + str(final_df_only_manual['REKORDY ZAAKCEPTOWANE'].replace(to_replace='None', value=np.nan).dropna().sum())) #Trochę więcej. Sprawdzić ze statystykami

### KOD PONIŻEJ GENERUJE LICZBY KTÓRE NIE ZGADZAJĄ SIE Z TABELAMI. ZBADAC SPRAWĘ
# print('Zeskrobane rekordy (do opracowania manualnego): ' + str(final_df_only_manual['REKORDY POZYSKANE'].replace(to_replace='None', value=np.nan).dropna().astype('int64').sum()))  #151459 powinno być○ ponad 180 000 (2024-10-24) BŁ◘AD bo niektóre są zeskrobane do automatycznego opracowania
# print('Zeskrobane rekordy (wszystkie): ' + str(final_df['REKORDY POZYSKANE'].replace(to_replace='None', value=np.nan).dropna().str.replace('\D', '', regex=True).astype('int64').sum())) 
      

#POPRAWIĆ FUNKCJĘ update_rekordy_zaakceptowane


#ZAPISZ RAPORT w jakim formacie i wyslij do arkusza





#Wyrzucenie zbędnych dla uczenia maszynowego kolumn:
machine_learning_df = final_df_only_manual.drop(columns=['CZY DO PRAC MANUALNYCH (PO ZMIANACH)', 'czy_przekazano_do_manual', 'CZY POZYSKANO?', 'OSOBA OPRACOWUJĄCA', 'STATUS PRAC'])
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






# def update_rekordy_zaakceptowane(df):
#     for index, row in tqdm(df.iterrows()):  
#         try:
#             link = row['LINK']
#             if not pd.isna(link):  # Check if 'LINK' is not null
#                 link = 'https://docs.google.com/spreadsheets/d/1zOxDVFLvk2ovJEyyKp-Zpw6Vqdqr3WG-nueOLy5cVs4/edit#gid=652340147'
#                 gsheetId = re.search('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/)[A-Za-z\d\_\-]*', link).group(0)
#                 table_df = gsheet_to_df(gsheetId, 'Posts')
#                 all_extracted_records = table_df['Link'].dropna().shape[0]
#                 df.at[index, 'REKORDY POZYSKANE'] = all_extracted_records
               
#                 filtered_df = table_df[(table_df['do PBL'] == 'True') & (table_df["Data publikacji"].notna())]
#                 accepted_records = len(filtered_df['Link'].tolist())
#                 df.at[index, 'REKORDY ZAAKCEPTOWANE'] = accepted_records
                        
#             else:
#                 df.at[index, 'REKORDY ZAAKCEPTOWANE'] = 'BRAK PRZYGOTOWANEJ TABELI'  
#                 df.at[index, 'REKORDY POZYSKANE'] = 'BRAK PRZYGOTOWANEJ TABELI'
       
#         except (KeyError, TypeError):
#             df.at[index, 'REKORDY ZAAKCEPTOWANE'] = None  
#             df.at[index, 'REKORDY POZYSKANE'] = None
#         except:
#             df.at[index, 'REKORDY ZAAKCEPTOWANE'] = "DO UZUPEŁNIENIA RĘCZNIE?" 
#             df.at[index, 'REKORDY POZYSKANE'] = "DO UZUPEŁNIENIA RĘCZNIE?" 

#     return df 





