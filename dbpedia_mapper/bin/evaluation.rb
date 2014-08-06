require 'csv'
require_relative '../lib/dbpedia_mapper'
require_relative '../lib/dbpedia_mapper/virtuoso'

DEFAULT_PATH = '../demo/1000_Movies.tsv'

def parse_movie_file(path)
  p path
  movie_links = []
  CSV.foreach(path, { col_sep: "\t" }) do |dbpedia_link, _, _|
    movie_links << dbpedia_link
  end
  movie_links
end

if ARGV.first
  DBpediaMapper::Mapper.new.start_demo parse_movie_file(ARGV.first)
elsif File.exists? DEFAULT_PATH
  DBpediaMapper::Mapper.new.start_demo parse_movie_file(File.absolute_path DEFAULT_PATH)
else
  p "please specify a tsv file"
end
