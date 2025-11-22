import requests
from bs4 import BeautifulSoup
import json
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
NUMERO_LIBROS = 15

def fetch_page(books, session):
    page = 1
    pages = int(books / 10 + 1)
    responses = []

    while page <= pages:
        SEARCH_URL = f"https://www.goodreads.com/search?page={page}&q=data+science&qid=F5bWyOVpkC&tab=books"
        response = session.get(SEARCH_URL)
        response.raise_for_status()
        responses.append(response.text)
        page += 1

    return responses

def get_ids_books(html):
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table.tableList tr")

    for row in rows:
        if len(ids) == NUMERO_LIBROS:
            break
        
        link = row.select_one("a")
        if link:
            href = link["href"]
            match = re.search(r"book/show/(\d+)",href)
            if match:
                book_id = match.group(1)
                ids.append(book_id)

def get_books(ids, session):
    books = []
    
    for id in ids:
        URL_SHOW_BOOK = f"https://www.goodreads.com/book/show/{id}"
        response = session.get(URL_SHOW_BOOK)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")  

        title = soup.select_one("h1.Text__title1").text.split(':')[0].strip()

        authorsRaw = soup.select("span.ContributorLink__name")
        authors = []
        for author in authorsRaw:
            if author.text not in authors:
                authors.append(author.text.strip())

        rating = soup.select_one("div.RatingStatistics__rating").text.strip()
        rating_count_raw = soup.select_one('[data-testid="ratingsCount"]').text.strip()
        rating_count_cleaned = rating_count_raw.replace("\xa0", " ") 
        rating_count = int(rating_count_cleaned.split(" ")[0].replace(",", "")) 

        script_tag = soup.select_one('script[type="application/ld+json"]')
        isbn13 = json.loads(script_tag.string).get("isbn", None)
        book = {
            "id":len(books) + 1,
            "title": title,
            "author": authors,
            "rating": rating,
            "ratings_count": rating_count,
            "book_url": URL_SHOW_BOOK,
            "ISBN_13": isbn13
        }

        books.append(book)
        print("Scrapeando: " + str(len(books)) + "/" + str(NUMERO_LIBROS))

    return books

session = requests.Session()
session.headers.update(HEADERS)

html_pages = fetch_page(NUMERO_LIBROS, session)

ids = []
for html in html_pages:
    get_ids_books(html)

all_books = get_books(ids, session)

with open("../landing/goodreads_books.json", "w", encoding="utf-8") as f:
        json.dump(all_books, f, ensure_ascii=False, indent=2)