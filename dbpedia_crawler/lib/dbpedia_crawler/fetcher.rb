# encoding: utf-8

require 'linkeddata'
require 'yaml'

# A fetcher requests data on particular entities according to fetching rules. 
# These rules specify which linked entities should be fetched as well.
class DBpediaCrawler::Fetcher

private

  #
  # fetching rules
  #

  # path to the YAML file containing the rules for fetching
  FETCHING_RULES_FILE = "../configuration/fetching_rules.yml"

  # Load the rules for fetching
  def load_rules
    @rules = YAML.load_file File::expand_path(FETCHING_RULES_FILE, __FILE__)
  end

  #
  # SPARQL queries
  #

  # path of the folder with the queries, ending with "/"
  QUERIES_PATH = File::expand_path("../queries", __FILE__) + "/"
  # file extension of files with queries
  QUERIES_FILE_EXT = ".txt"

  # symbols which denote queries
  QUERIES = [
    :count_movies,  # query number of distinct movies
    :movies,        # query a page of movies
    :count_shows,   # query number of distinct shows
    :shows          # query a page of shows
  ]

  # load query strings from files
  def initialize_queries
    @queries = {}
    QUERIES.each do |symbol|
      File.open(QUERIES_PATH + symbol.to_s + QUERIES_FILE_EXT, "r") do |file|
        @queries[symbol] = file.read
      end
    end
  end

public

  # Create a new fetcher which acts on the given source
  #   source: DBpediaCrawler::Source
  def initialize(source)
    # get the source
    @source = source
    # load the rules
    load_rules
    # load query strings
    initialize_queries
  end

  # Fetch the data about the entity identified by the given URI depending on
  # the rules for the given type.
  #   uri: a URI (uri.to_s must be a valid URI)
  #   type: string
  #   result: RDF::Graph
  def fetch(uri, type)
    # TODO: extend naive implementation
    return @source.triples_for(uri)
  end

  # Query all IDs of entities which are recognized as movies.
  #   result: array
  def query_movie_ids
    return @source.query_with_pagination(@queries[:movies], @queries[:count_movies])
  end

  # Query all IDs of entities which are recognized as TV shows.
  #   result: array
  def query_show_ids
    return @source.query_with_pagination(@queries[:shows], @queries[:count_shows])
  end

end
