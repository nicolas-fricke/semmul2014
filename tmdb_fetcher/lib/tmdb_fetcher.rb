require_relative 'tmdb_fetcher/fetcher'
require_relative 'tmdb_fetcher/msg_publisher'

movie_fetcher = Fetcher.new
puts movie_fetcher.fetch_movie_ids verbose: true

# msg_publisher = MsgPublisher.new
# msg_publisher.enqueue_id :movie_id, 123