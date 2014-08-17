require 'csv'
require_relative '../lib/tmdb_mapper'
DEFAULT_PATH = '../demo/1000_Movies.tsv'

def parse_movie_file(path)
  movie_links = []
  CSV.foreach(path, { :col_sep => "\t" }) do |_, _, tmdb_uri|
    movie_links << tmdb_uri
  end
  movie_links
end


if ARGV.first
  TMDbMapper::Mapper.new.start_demo parse_movie_file(ARGV.first)
elsif File.exists? DEFAULT_PATH
  TMDbMapper::Mapper.new.start_demo parse_movie_file(File.absolute_path DEFAULT_PATH)
else
  p "please specify a cvs file"
end
