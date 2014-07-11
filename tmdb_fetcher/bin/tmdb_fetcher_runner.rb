require_relative '../lib/tmdb_fetcher'

if ARGV[0] == 'demo' and not ARGV[1].nil?
  puts 'Started fetcher in demo mode. Please type [yes] for fetching the demo movies.'
  TMDbFetcher::Demo.new(ARGV[1]) if STDIN.gets(3) == 'yes'
else
  movie_fetcher = TMDbFetcher::Fetcher.new
  puts movie_fetcher.fetch_movie_ids verbose: true
end
