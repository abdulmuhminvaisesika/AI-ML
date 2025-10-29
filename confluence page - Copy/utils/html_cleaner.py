# utils/html_cleaner.py
from bs4 import BeautifulSoup

def clean_html_to_text(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator="\n")  # Keeps structure readable
    return text.strip()
