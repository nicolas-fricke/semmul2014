require_relative 'tmdb_fetcher/fetcher'
require_relative 'tmdb_fetcher/msg_publisher'

movie_fetcher = Fetcher.new
puts movie_fetcher.fetch_movie_ids verbose: true
