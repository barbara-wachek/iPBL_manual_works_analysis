
# import requests
# from bs4 import BeautifulSoup
# import time
# import os
# import re

# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from webdriver_manager.chrome import ChromeDriverManager

#%% base search and link gathering 

# def google_search(query, start=0):
    
#     query = 'teatr'
#     start = 0
#     # Przygotuj zapytanie do wyszukiwarki Google z parametrem start
#     search_url = f"https://www.google.com/search?q={query}&num=100&start={start}"
#     # headers = {
#     #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
#     # }
    
    
#     options = Options()
#     options.add_argument("--headless")
#     # options.add_argument("--no-sandbox")
#     # options.add_argument("--disable-dev-shm-usage")

#     driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
#     driver.get(search_url)
#     time.sleep(2)
    
    
#     response = driver.page_source
    
    
#     driver.quit()
    
    
#     return response


# def extract_links(html):
#     # Sprawdź, czy HTML nie jest pusty
#     if html is None:
#         print("HTML is None, returning empty list.")
#         return []
    
#     # Przetwarzanie HTML i wyciąganie linków
#     soup = BeautifulSoup(html, 'html.parser')
#     links = []
#     # keywords = ["program", "agenda", "harmonogram", "rozkład", "grafik"]
    
#     print("Starting to extract links...")
#     for item in soup.find_all('a'):
#         href = item.get('href')
#         if href and 'http' in href:
#             # Zapisz linki zawierające słowa kluczowe
#             # if any(keyword in href.lower() for keyword in keywords):
#             links.append(href)
#             print(f"Link added: {href}")
    
#     # Debug: Wydrukuj liczbę znalezionych linków
#     print(f"Found {len(links)} links after processing.")
    
#     return links


# def main():
#     # years = range(2000, 2025) 
#     base_queries = [  
#         'teatr'
#     ]

#     all_links = []
    
#     # Tworzenie zapytań dla różnych lat

#     for query in base_queries:
#         for start in range(0, 200, 100):  # Stronicowanie, aby uzyskać do 300 wyników (z 3 stron)
#             print(f"Searching for: {query} (start={start})")
#             html = google_search(query, start=start)
            
#             if html:
#                 print("HTML content retrieved, length:", len(html))
#             else:
#                 print("Failed to retrieve HTML content.")
            
#             links = extract_links(html)
#             all_links.extend(links)
            
#             time.sleep(55)  # Aby uniknąć blokady IP

#     # Usuń duplikaty
#     unique_links = list(set(all_links))

#     # Zapisz linki do pliku
#     with open("event_program_links.txt", "w") as file:
#         for link in unique_links:
#             file.write(f"{link}\n")

#     print(f"Found {len(unique_links)} unique links.")
    
#     return

# main()






#%% e-teatr

from bs4 import BeautifulSoup
import time
from concurrent.futures import ThreadPoolExecutor
import requests

import pandas as pd
import regex as re
from tqdm import tqdm  #licznik
from datetime import datetime


#%% def

def get_theatres(link): 
    html_text = requests.get(link).text

    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    
    soup = BeautifulSoup(html_text, 'lxml')
    
    
    for h5 in soup.find_all("h5"): 
        header_text = h5.get_text(strip=True)
        if h5.text != 'Wydawca i redakcja':
            div = h5.find_next_sibling('div', class_='three-columns border border-b')
    
            links = [{"Nazwa": x.p.get_text(strip=True), "Link": "https://e-teatr.pl" + x.get('href'), 'Typ': header_text} for x in div.find_all('a')]
            all_links.extend(links)



def update_theatre_info(item):   
    link = item.get('Link')
    if not link: 
        return item
    
    try:
    
        html_text = requests.get(link).text   
        
        while 'Error 503' in html_text:
            time.sleep(2)
            html_text = requests.get(link).text
        
        soup = BeautifulSoup(html_text, 'lxml')
    
        # theatre_link = " | ".join([x.get('href') for x in soup.find('div', class_='billboard-info-cast').find_all('a') if x.get('href').startswith('http')])
        # address = soup.find('address').get_text(strip=True)

        cast_div = soup.find('div', class_='billboard-info-cast')
        
        if cast_div:
            theatre_link = " | ".join(
                [a.get('href') for a in cast_div.find_all('a') if a.get('href', '').startswith('http')]
            )
        else:
            theatre_link = None
            
            # pobranie adresu
        address_tag = soup.find('address')
        address = address_tag.get_text(strip=True) if address_tag else None
        
        item['Strona teatru'] = theatre_link
        item['Adres'] = address
        if address:
            match = re.search(r'\d{2}-\d{3}\s+(.*)', address)
            if match:
                item['Miasto'] = match.group(1).strip()
            else:
                item['Miasto'] = None
        else:
            item['Miasto'] = None
        
    except:      
        print('Nie udało się')




#%% main

baza_adresowa_links = ['https://e-teatr.pl/baza-adresowa-wielkopolskie', 'https://e-teatr.pl/baza-adresowa-kujawsko-pomorskie', 'https://e-teatr.pl/baza-adresowa-dolnoslaskie', 'https://e-teatr.pl/baza-adresowa-lubelskie', 'https://e-teatr.pl/baza-adresowa-lodzkie', 'https://e-teatr.pl/baza-adresowa-malopolskie', 'https://e-teatr.pl/baza-adresowa-mazowieckie', 'https://e-teatr.pl/baza-adresowa-opolskie', 'https://e-teatr.pl/baza-adresowa-podkarpackie', 'https://e-teatr.pl/baza-adresowa-podlaskie', 'https://e-teatr.pl/baza-adresowa-pomorskie', 'https://e-teatr.pl/baza-adresowa-slaskie', 'https://e-teatr.pl/baza-adresowa-swietokrzyskie', 'https://e-teatr.pl/baza-adresowa-warminsko-mazurskie', 'https://e-teatr.pl/baza-adresowa-zachodniopomorskie']

all_links = []

with ThreadPoolExecutor() as executor: 
    list(tqdm(executor.map(get_theatres, baza_adresowa_links),total=len(baza_adresowa_links)))      


with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(update_theatre_info, all_links),
                              total=len(all_links)))

df = pd.DataFrame(all_links).drop_duplicates()


with pd.ExcelWriter(f"data/e-teatr_teatry_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     






























