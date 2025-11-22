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
    # Si la búsqueda no devuelve resultados, generar un registro artificial
    if not book_api_google:
        artificial = {
            "id": query.get("id"),
            "gb_id": None,
            "language": None,
            "title": None,
            "subtitle": None,
            "publisher": None,
            "published_date": None,
            "price_amount": None,
            "price_currency": None,
            "ISBN_10": None,
            "ISBN_13": None,
            "book_url": None
        }
        books_data.append(artificial)
        count += 1
        print("-" * 20 + " Generado registro artificial para id: " + str(query.get("id")) + " " + "-" * 20)
    else:
        for book in book_api_google:
            gb_id = book.get("id")
            id = book.get("id_scrapper")

            volume_info = book.get('volumeInfo', {})
            title = volume_info.get('title', None)
            subtitle = volume_info.get('subtitle', None)
            publisher = volume_info.get('publisher', None)
            published_date = volume_info.get('publishedDate', None)
            language = volume_info.get("language", None)

            sale_info = book.get('saleInfo', {})
            retail_price = sale_info.get('retailPrice', {})
            price_amount = retail_price.get('amount', None)
            price_currency = retail_price.get('currencyCode', None)

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
                "price_amount": price_amount,
                "price_currency": price_currency,
                "ISBN_10": isbn_10,
                "ISBN_13": isbn_13,
                "book_url": googe_books_url
            }

            books_data.append(book_details)
            count += 1
            print("-" * 20 + " Cargado libro: " + str(count) + " " + "-" * 20)

# =============================================================
#                        PASAR A CSV
# =============================================================
CSV_PATH = pathlib.Path("..") / "landing" / "googlebooks_books.csv"

with open(CSV_PATH, mode="w", encoding="utf-8", newline="") as csvfile:
    fieldnames = [
        "id",
        "gb_id",
        "language",
        "title",
        "subtitle",
        "publisher",
        "published_date",
        "price_amount",
        "price_currency",
        "ISBN_10",
        "ISBN_13",
        "book_url",
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    if books_data:
        writer.writerows(books_data)






