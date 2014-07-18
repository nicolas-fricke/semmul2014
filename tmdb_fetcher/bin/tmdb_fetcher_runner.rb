require_relative '../lib/tmdb_fetcher'

movie_fetcher = TMDbFetcher::Fetcher.new
puts movie_fetcher.fetch_movie_ids verbose: true
