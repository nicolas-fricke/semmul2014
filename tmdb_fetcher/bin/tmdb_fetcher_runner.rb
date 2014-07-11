require_relative '../lib/tmdb_fetcher'

if ARGV[0] == 'demo' and not ARGV[1].nil?
  puts 'Started fetcher in demo mode. Please type [yes] for fetching the demo movies.'
  if STDIN.gets(3) == 'yes'
    TMDbFetcher::Demo.new(ARGV[1])
    puts 'Enqueued demo movies!'
  else
    puts 'Aborted!'
  end
else
  movie_fetcher = TMDbFetcher::Fetcher.new
  puts movie_fetcher.fetch_movie_ids verbose: true
end
