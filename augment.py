import csv
import requests
import sys
import pathlib
import os.path
import xml.etree.ElementTree
import re

KEY = sys.argv[1]

GOODREADS = 'https://www.goodreads.com/book/isbn/%s?key=' + KEY

pathlib.Path('.cache').mkdir(exist_ok=True)

def find_document(isbn):
  filename = '.cache/%s.xml' % isbn
  if os.path.exists(filename):
    with open(filename, 'r') as f:
      return f.read()
  padded = ('0' * (10 - len(isbn))) + isbn
  resp = requests.get(GOODREADS % padded)
  print(resp)
  if resp.status_code == 200:
    body = resp.text
    with open(filename, 'w') as f:
      f.write(body)
    return body

def parse_document(data):
  parsed = {}
  if not data:
    return parsed
  doc = xml.etree.ElementTree.fromstring(data)
  for elem in doc.findall('book/description'):
    if elem.text:
      parsed['description'] = re.sub("<.+>", "", elem.text)
  for elem in doc.findall('book/publisher'):
    parsed['publisher'] = elem.text
  parsed['genres'] = []
  for elem in doc.findall('book/popular_shelves/shelf'):
    parsed['genres'].append(elem.attrib.get('name'))
  return parsed

  
def augment_books(fl):
  with open(fl, 'r') as bcsv:
    rdr = csv.DictReader(bcsv)
    for row in rdr:
      isbn = row['isbn']
      if isbn:
        extra_raw = find_document(isbn)
        extra = parse_document(extra_raw)
        row.update(extra)
      yield row
  
def fields(rows):
  fields = set([])
  for row in rows:
    fields.update(set(row.keys()))
  return fields


def write_books(rows, out):
  fls = fields(rows)
  with open(out, 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fls)
    writer.writeheader()
    for row in rows:
      writer.writerow(row)

augmented = list(augment_books('books.csv'))
write_books(augmented, 'books_expanded.csv')
