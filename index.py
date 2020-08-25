import csv

spl = lambda s: [x.strip() for x in s.split(',') if x]

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
  'small_image_url': str
}

def read_books(fl):
  with open(fl, 'r') as bcsv:
    rdr = csv.DictReader(bcsv)
    for row in rdr:
      target = {}
      for field, conv in SCHEMA.items():
        if field in row:
          try:
            target[field] = conv(row[field])
          except:
            pass
      yield target

books = read_books('books.csv')
