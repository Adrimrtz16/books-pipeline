import json
import csv
from pathlib import Path
import pandas as pd

with open("../landing/goodreads_books.json", "r", encoding="utf-8") as file:
    goodreads_books = json.load(file)

with open("../landing/googlebooks_books.csv", "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    googlebooks_books = [row for row in reader]

def prefer(key):
    if isinstance(book_google, dict):
        val = book_google.get(key)
        if val is not None and str(val).strip() != "":
            return val
    if isinstance(book_goodreads, dict):
        val = book_goodreads.get(key)
        if val is not None and str(val).strip() != "":
            return val
    return None

merge_books = []
details = []

for i in range(len(goodreads_books)):
    book_google = googlebooks_books[i] 
    book_goodreads = goodreads_books[i]

    book = {
        "canonical_id": book_google.get('id', None),
        "gb_id": book_google.get('gb_id', None),
        "title": prefer("title"),
        "subtitle": book_google.get('subtitle', None),
        "author": prefer("author"),
        "publisher": book_google.get('publisher', None),
        "published_date": book_google.get('published_date', None),
        "rating": book_goodreads.get('rating', None),
        "ratings_count": book_goodreads.get('ratings_count', None),
        "ISBN_10": book_google.get('ISBN_10', None),
        "ISBN_13": prefer("ISBN_13"),
        "price_amount": book_google.get('price_amount', None),
        "price_currency": book_google.get('price_currency', None),
        "book_url_goodreads": book_goodreads.get('book_url', None),
        "book_url_google_books": book_google.get('book_url', None),
    }

    detail = {
        "canonical_id": book_google.get('id', None),
        "merge_with_google_books": bool(book_google.get('gb_id')),
        "merge_by": "id",
        "timestamp": pd.Timestamp.now(tz="UTC").isoformat()
    }

    merge_books.append(book)
    details.append(detail)

OUTPUT_DIR = Path("../standard/dim_book.parquet")
df = pd.DataFrame(merge_books)
df.to_parquet(OUTPUT_DIR, index=False)
print("Creado dim_book.parquet")

OUTPUT_DIR = Path("../standard/book_source_detail.parquet")
df = pd.DataFrame(details)
df.to_parquet(OUTPUT_DIR, index=False)
print("Creado book_source_detail.parquet")

OUTPUT_DIR = Path("../docs/quality_metrics.json")
total = len(details)
merge_true = sum(1 for r in details if r.get("merge_with_google_books"))
merge_false = total - merge_true

metrics = {
    "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
    "rows_input": total,
    "rows_output": total,
    "matched_with_google": merge_true,
    "percent_merge_true": (merge_true / total * 100) if total else 0,
    "percent_merge_false": (merge_false / total * 100) if total else 0,
    "duplicates_removed": 0,
    "source_preference_counts": {
        "google": merge_true,
        "goodreads_only": merge_false
    }
}

with OUTPUT_DIR.open("w", encoding="utf-8") as f:
    json.dump(metrics, f, ensure_ascii=False, indent=2)

print("Creado quality_metrics.json")