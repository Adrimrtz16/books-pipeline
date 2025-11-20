import os
from dotenv import load_dotenv

load_dotenv()
GOOGLE_BOOKS_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")

if not GOOGLE_BOOKS_API_KEY:
    raise ValueError("La variable de entorno GOOGLE_BOOKS_API_KEY no está configurada.")

print(f"Tu API key es: {GOOGLE_BOOKS_API_KEY}")