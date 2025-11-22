import os
import requests
from dotenv import load_dotenv
import json
import pathlib
import time
import csv

load_dotenv()
GOOGLE_BOOKS_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")

if not GOOGLE_BOOKS_API_KEY:
    raise ValueError("La variable de entorno GOOGLE_BOOKS_API_KEY no está configurada.")

def search_book(query_info, max_results=1, retries=3, delay=2):
    url = "https://www.googleapis.com/books/v1/volumes"
    if query_info["ISBN_13"] != None:
        q = f"isbn:{query_info["ISBN_13"]}"
    else:
        q = f"intitle:{query_info["title"]}"

    params = {
        "q": q,
        "maxResults": max_results,  
        "key": GOOGLE_BOOKS_API_KEY 
    }

    attempts = 0
    while attempts < retries:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            for item in data.get("items", []):
                item["id_scrapper"] = query_info["id"]
            if q == f"intitle:{query_info['title']}":
                for book in data.get("items", []): 
                    authors = book.get('volumeInfo', {}).get('authors', [])
                    if set(authors) == set(query_info["author"]): 
                        return data.get("items", [])
                return []
            return data.get("items", [])
        elif response.status_code in [500, 503]:
            attempts += 1
            time.sleep(delay) 
        else:
            raise Exception(f"Error {response.status_code}: {response.text}")

    raise Exception(f"Error {response.status_code}: {response.text}. Se alcanzó el límite de reintentos.")

# =============================================================
#                     SACAR INFO DEL JSON
# =============================================================
JSON_PATH = pathlib.Path("..") / "landing" / "goodreads_books.json"

with open(JSON_PATH, "r", encoding="utf-8") as file:
    books_data = json.load(file)

books_isbn_title = []

for book in books_data:
    book_info = {
        "id":  book["id"],
        "ISBN_13": book["ISBN_13"],
        "title": book["title"],
        "author": book["author"]
    }
    books_isbn_title.append(book_info)

# =============================================================
#                 BUSCAR EN GOOGLE BOOKS API
# =============================================================
books_data = []
count = 0
for query in books_isbn_title:
    book_api_google = search_book(query)
    for book in book_api_google:
        gb_id = book.get("id")
        id = book.get("id_scrapper")

        volume_info = book.get('volumeInfo', {})
        title = volume_info.get('title', None)
        subtitle = volume_info.get('subtitle', None)
        publisher = volume_info.get('publisher', None)
        published_date = volume_info.get('publishedDate', None)
        language = volume_info.get("language", None)

        sale_info = book.get('saleInfo', [])
        retail_price = sale_info.get('retailPrice', {})
        list_price = sale_info.get('listPrice', {})
        retail_price = f"{retail_price.get('amount')} {retail_price.get('currencyCode')}"
        list_price = f"{list_price.get('amount')} {list_price.get('currencyCode')}"
        if str(retail_price) == "None None":
            retail_price = None
        if str(list_price) == "None None":
            list_price = None

        isbn_list = volume_info.get('industryIdentifiers', [])
        isbn_10 = next((isbn['identifier'] for isbn in isbn_list if isbn['type'] == 'ISBN_10'), None)
        isbn_13 = next((isbn['identifier'] for isbn in isbn_list if isbn['type'] == 'ISBN_13'), None)

        googe_books_url = book.get('selfLink')

        book_details = {
            "id": id,
            "gb_id": gb_id,
            "language": language,
            "title": title,
            "subtitle": subtitle,
            "publisher": publisher,
            "published_date": published_date,
            "retail_price": retail_price,
            "list_price": list_price,
            "isbn_10": isbn_10,
            "isbn_13": isbn_13,
            "book_url": googe_books_url
        }

        books_data.append(book_details)
        count += 1
        print("-" * 20 + " Cargado libro: " + str(count) + " " + "-" * 20)

# =============================================================
#                 BUSCAR EN GOOGLE BOOKS API
# =============================================================
CSV_PATH = pathlib.Path("..") / "landing" / "googlebooks_books.csv"

with open(CSV_PATH, mode="w", encoding="utf-8", newline="") as csvfile:
    fieldnames = books_data[0].keys() 
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()  
    writer.writerows(books_data)  






