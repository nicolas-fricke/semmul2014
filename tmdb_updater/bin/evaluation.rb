require 'csv'
require_relative '../lib/tmdb_updater'

def parse_movie_file(path)
  p path
  movie_links = []
  CSV.foreach(path, { :col_sep => "\t" }) do |_, _, tmdb_link|
    movie_links << tmdb_link
  end
  movie_links.map do |l|
    l.match /.*\/(\d*)[^\d]*/
    $1
  end.reject {|l| l.nil? }
end

DEFAULT_PATH = '../demo/1000_Movies.tsv'

if ARGV.first
  TMDbUpdater::Updater.new.start_demo parse_movie_file(ARGV.first)
elsif File.exists? DEFAULT_PATH
  TMDbUpdater::Updater.new.start_demo parse_movie_file(File.absolute_path DEFAULT_PATH)
else
  p "please specify a cvs file"
end