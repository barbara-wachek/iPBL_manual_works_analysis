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

#%% 

dokumentacja_zrodla_internetowe = gsheet_to_df('1nw7bwi15rm-0BAc_Hf47PX_qBEPV1Wjwhn1-mpebyx4', 'iPBL – źródła internetowe')
dokumentacja_zrodla_internetowe.columns


dokumentacja_web_scraping_df = gsheet_to_df('1-g_pgkvRIhBSKBENu_5HhRCMsHatv-eux3U_ERGHZG0', 'Raport')
dokumentacja_web_scraping_df = dokumentacja_web_scraping_df.rename(columns={'LINK DO STRONY':'Adres'})



merged_df = pd.merge(dokumentacja_zrodla_internetowe, dokumentacja_web_scraping_df, on='Adres', how='left')


#Uwzględnienie tylko dostępnych źródeł do opracowania:
dokumentacja_zrodla_internetowe_only_available = merged_df.loc[(merged_df['CZY POZYSKANO?'] != 'REZYGNACJA') & (merged_df['CZY POZYSKANO?'] != 'NIEDOSTĘPNA')]


result = dokumentacja_zrodla_internetowe_only_available.groupby(['Dziedzina', 'Typ']).size().unstack(fill_value=0)
result['Czasopisma_test'] = result['Czasopisma'] + result['Czasopismo']
result = result.drop(columns=['Czasopisma', 'Czasopismo'])
result = result.rename(columns={'Czasopisma_test': 'Czasopisma'})
# Normalizacja do wartości procentowych
result = result.div(result.sum(axis=1), axis=0) * 100
result['Suma'] = result.sum(axis=1)  
# Formatowanie wyników (opcjonalnie)
result = result.round(2)  # Zaokrąglenie do dwóch miejsc po przecinku
print(result)


for col in result.columns:
    result[col] = result[col].astype(str) + '%'
    

 
with pd.ExcelWriter(f"data\iPBL_sources_analysis_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    result.to_excel(writer, index=True, encoding='utf-8')   
    writer.save()      
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    