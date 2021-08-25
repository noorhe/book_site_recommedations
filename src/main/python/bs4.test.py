from bs4 import BeautifulSoup as bs
import requests

req = requests.get("https://www.{book_site_name}.ru/reader/GuenetteUnremitted/read")
soup = bs(req.text, 'html.parser')
print(soup.prettify())
