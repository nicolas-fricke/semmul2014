require 'csv'
require_relative '../../general/virtuoso_reader'
require_relative '../lib/matcher'

def evaluate(path)
  p path
  counter = 1
  reader = VirtuosoReader.new graph:'mapped'
  matcher = Matcher::Matcher.new(true)

  match_db_fb = [0,0] # match, no_match

  CSV.foreach(path, { :col_sep => "\t" }) do |dbpedia_link, fb_link, tmdb_link|

    begin

      # subject uris
      dbpedia_exists = reader.exists_subject subject: dbpedia_link
      fb_mid = fb_link.split("freebase.com/m/")[1]
      fb_uri = "http://rdf.freebase.com/ns/m/#{fb_mid}"
      fb_exists = reader.exists_subject subject: fb_uri
      tmdb_id = tmdb_link.split("themoviedb.org/")[1].split("/")[1][/\d+/]
      tmdb_uri = "http://semmul2014.hpi.de/tmdb/movie/#{tmdb_id}"
      tmdb_exists = reader.exists_subject subject: tmdb_uri

      if dbpedia_exists and fb_exists
        match01 = matcher.evaluation_match(dbpedia_link, fb_uri)
      end


    rescue Exception => e
      puts e
    end
    $stdout.write "\r#{counter}/1000"
    $stdout.flush
    counter += 1
  end
  puts ""

end

DEFAULT_PATH = '../demo/1000_Movies.tsv'


evaluate(File.absolute_path DEFAULT_PATH)
