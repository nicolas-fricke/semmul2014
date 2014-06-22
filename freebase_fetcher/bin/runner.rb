# Fetcher
require_relative '../lib/freebase_fetcher'

movie_fetcher = FreebaseFetcher::Fetcher.new
movie_fetcher.retrieve_film_ids verbose: true
