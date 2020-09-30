import csv
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json

spl = lambda s: [x.strip() for x in s.split(',') if x]

js = lambda x: json.loads(x.replace("'", '"'))

SCHEMA = {
  'book_id': str,
  'isbn': str,
  'isbn13': str,
  'original_publication_year': float,
  'title': str,
  'authors': spl,
  'language_code': str,
  'average_rating': float,
  'ratings_count': int,
  'ratings_1': int,
  'ratings_2': int,
  'ratings_3': int,
  'ratings_4': int,
  'ratings_5': int,
  'image_url': str,
  'small_image_url': str,
  'description': str,
  'publisher': str,
  'genres': js
}

INDEX = "books"

es = Elasticsearch()

def read_books(fl):
  with open(fl, 'r') as bcsv:
    rdr = csv.DictReader(bcsv)
    for row in rdr:
      target = {}
      for field, conv in SCHEMA.items():
        if field in row:
          try:
            target[field] = conv(row[field])
          except Exception as e:
            print(e)
      yield {
        "_index": INDEX,
        "_id": target['book_id'],
        "_source": target
      }

def send_books(books):
  resp = helpers.bulk(es, books)
  es.indices.refresh(index=INDEX)

def setup_index(name):
  with open('books_schema.json', 'r') as f:
    es.indices.create(index=name, body=f.read())
  
books = read_books('books_expanded.csv')
setup_index(INDEX)
send_books(books)
