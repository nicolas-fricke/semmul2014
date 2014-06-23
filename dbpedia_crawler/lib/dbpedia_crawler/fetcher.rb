# encoding: utf-8

require 'linkeddata'
require 'yaml'

# A fetcher requests data on particular entities according to fetching rules. 
# These rules specify which linked entities should be fetched as well.
#
# Problem: The server error
#   "Virtuoso 42001 Error SR185: Undefined procedure DB.SPARQL.isgeometry"
# occurs when RDF data with certain geospatial data like
#   dbpedia:United_States 
#   <http://www.w3.org/2003/01/geo/wgs84_pos#geometry>
#   "POINT(-77.016670 38.883335)"^^<http://www.openlinksw.com/schemas/virtrdf#Geometry>
# is attempted to be inserted in a Virtuoso store which is open source 
# (the procedure is limited to the licenced edition).
# See http://sourceforge.net/p/virtuoso/mailman/message/28874876/.
class DBpediaCrawler::Fetcher

private

  #
  # fetching rules
  #

  # path to the YAML file containing the rules for fetching
  FETCHING_RULES_FILE = "../configuration/fetching_rules.yml"

  # Load the rules for fetching
  def load_rules
    return YAML.load_file(File::expand_path(FETCHING_RULES_FILE, __FILE__))
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
  def load_queries
    queries = {}
    QUERIES.each do |symbol|
      File.open(QUERIES_PATH + symbol.to_s + QUERIES_FILE_EXT, "r") do |file|
        queries[symbol] = file.read
      end
    end
    return queries
  end

  # Remove unwanted triples from the given data.
  # Changes to the given graph are made in place.
  #   data: RDF::Graph
  #   result: RDF::Graph (the given graph)
  def filter(data)
    # find statements to delete
    unwanted = RDF::Graph.new
    data.statements.each do |statement|
      # filter <http://www.openlinksw.com/schemas/virtrdf#Geometry>
      o = statement.object
      if o.literal? and o.datatype.to_s == "http://www.openlinksw.com/schemas/virtrdf#Geometry"
        puts "  Filtered statement with literal #{o}."
        unwanted << statement
      end
    end
    # delete unwanted statements
    data.delete unwanted
  end

public

  # Create a new fetcher which acts on the given source
  #   source: DBpediaCrawler::Source
  def initialize(source)
    # get the source
    @source = source
    # load the rules
    @rules = load_rules
    # load query strings
    @queries = load_queries
  end

  #
  # fetching
  #

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

  # Fetch the data about the entity identified by the given URI depending on
  # the rules for the given type.
  # To cope with very large data sets, yet allow for a stateful execution,
  # the method does not return an aggregated graph yet yields a graph for
  # every related resource. 
  # One execution of this method will not cause the same resource to be 
  # fetched twice.
  #   uri: a URI (uri.to_s must be a valid URI of an entity)
  #   type: string (type for which rules are defined)
  #   yields: RDF::GRAPH
  def fetch(uri, type, &block)
    # initialize: entity noted, fetch given entity
    noted_uris, stack = [uri.to_s], [[uri.to_s, type.to_s]]
    # fetch all data according to the rules
    until stack.empty?
      # fetch data of next entity
      uri, type = stack.pop
      raise "Skipping #{type} #{uri} (unkown type)." unless @rules.include?(type)
      puts "Fetching #{type} #{uri}..."
      data = @source.triples_for uri
      # search for other entities to fetch
      data.each do |triple|
        s, p, o = triple.subject.to_s, triple.predicate.to_s, triple.object
        # traverse triples whose subject is the entity (no inverse triples)
        if s == uri
          # if a rule matches and the object is a new URI, add it to the stack
          fetch_type = @rules[type].nil? ? nil : @rules[type][p]
          unless fetch_type.nil?
            if o.uri? and not noted_uris.include?(o.to_s)
              puts "  Adding #{fetch_type} #{o} to stack."
              stack << [o.to_s, fetch_type]
              noted_uris << o.to_s
            else
              puts "  Not adding #{fetch_type} #{o} (" + (o.uri? ? "already noted" : "not a URI") + ")."
            end
          end
        end
      end
      # yield graph of the fetched entity
      yield filter(data)
    end
  end

end
