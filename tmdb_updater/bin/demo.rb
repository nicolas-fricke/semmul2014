require 'csv'
require_relative '../lib/tmdb_updater'

def parse_movie_file(path)
  p path
  movie_links = []
  CSV.foreach(path) do |name, _, _, tmdb_link|
    movie_links << tmdb_link
  end
  movie_links.map do |l|
    l.match /.*\/(\d*)[^\d]*/
    $1
  end.reject {|l| l.nil? }
end

if ARGV.first
  TMDbUpdater::Updater.new.start_demo parse_movie_file(ARGV.first)
else
  p "please specify the path to the CSV file"
end

