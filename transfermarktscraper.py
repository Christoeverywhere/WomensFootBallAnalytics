"""
TM Search Debugger — run this first to see what HTML TM returns
"""
import requests
from bs4 import BeautifulSoup

try:
    import cloudscraper
    session = cloudscraper.create_scraper(browser={"browser":"chrome","platform":"windows","mobile":False})
    print("Using cloudscraper")
except:
    session = requests.Session()

session.headers.update({
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer":         "https://www.transfermarkt.co.uk/",
})

# Warm up
session.get("https://www.transfermarkt.co.uk/", timeout=15)
import time; time.sleep(3)

# Test search
name  = "Keira Walsh"
url   = f"https://www.transfermarkt.co.uk/schnellsuche/ergebnis/schnellsuche?query={name.replace(' ', '+')}&Spieler_page=0"
print(f"\nSearching: {url}\n")

r = session.get(url, timeout=15)
print(f"Status: {r.status_code}")

soup = BeautifulSoup(r.text, "lxml")

# Print all tables found
tables = soup.find_all("table")
print(f"Tables found: {len(tables)}")
for i, t in enumerate(tables):
    print(f"  Table {i}: class={t.get('class')} — rows={len(t.find_all('tr'))}")

# Print all h2 headers
for h in soup.find_all(["h2","h3"]):
    print(f"  Header: {h.get_text(strip=True)}")

# Print all links with /profil/spieler/
links = soup.find_all("a", href=lambda h: h and "/profil/spieler/" in str(h))
print(f"\nPlayer profile links found: {len(links)}")
for l in links[:5]:
    print(f"  {l['href']} — {l.get_text(strip=True)}")

# Save raw HTML for inspection
with open("tm_debug.html", "w", encoding="utf-8") as f:
    f.write(r.text)
print("\nFull HTML saved to tm_debug.html — open in browser to inspect")