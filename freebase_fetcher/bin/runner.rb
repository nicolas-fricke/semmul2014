require_relative '../lib/freebase_fetcher'

movie_fetcher = FreebaseFetcher::Fetcher.new
puts movie_fetcher.retrieve_film_ids
