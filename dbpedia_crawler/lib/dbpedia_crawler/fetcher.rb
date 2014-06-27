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
  #   uri: uri of the entity
  #   result: RDF::Graph (the given graph)
  def filter(data, uri)
    # find statements to delete
    unwanted = RDF::Graph.new
    data.statements.each do |statement|
      s, p, o = statement.subject, statement.predicate, statement.object
      # filter inverse triples (something p uri)
      unless s.to_s == uri
        unwanted << statement
        next
      end
      # filter <http://www.openlinksw.com/schemas/virtrdf#Geometry>
      if o.literal? and o.datatype.to_s == "http://www.openlinksw.com/schemas/virtrdf#Geometry"
        puts "  Filtered statement with literal #{o}."
        unwanted << statement
        next
      end
    end
    # delete unwanted statements
    data.delete unwanted
  end

  # Add a triple which assigns the ontology type to the entity.
  #   uri: uri of the entity
  #   type: string
  #   data: RDF::Graph
  def add_type(uri, type, data)
    triple = RDF::Statement.new(RDF::URI.new(uri.to_s), RDF.type, RDF::URI.new(@types[type]))
    data << triple
  end

  # Add a triple which documents the time of fetching.
  #   uri: uri of the entity
  #   data: RDF::Graph
  def add_provenance(uri, data)
    triple = RDF::Statement.new(RDF::URI.new(uri.to_s), \
      RDF::URI.new("http://purl.org/pav/lastUpdateOn"), \
      "#{DateTime.now}^^#{RDF::XSD.dateTime}")
    data << triple
  end

public

  # Create a new fetcher which acts on the given source
  #   source: DBpediaCrawler::Source
  #   types: hash
  #   rules: hash
  def initialize(source, types, rules)
    @source = source
    @types = types
    @rules = rules
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
  # If a type is not known (regarding both the ontology type and the fetching
  # rules), a StandardError will be raised.
  # The respective ontology type is added as a triple for all fetched entities.
  # The time of fetching is added as a triple for all fetched entities using
  # "http://purl.org/pav/lastUpdateOn".
  #   uri: a URI (uri.to_s must be a valid URI of an entity)
  #   type: string (type for which ontology type and rules are defined)
  #   yields: RDF::Graph
  #   raises: StandardError
  def fetch(uri, type, &block)
    # initialize: entity noted, fetch given entity
    noted_uris, stack = [uri.to_s], [[uri.to_s, type.to_s]]
    # fetch all data according to the rules
    until stack.empty?
      # fetch data of next entity
      uri, type = stack.pop
      raise "Skipping #{type} #{uri} (type not known)." unless @types.include?(type)
      raise "Skipping #{type} #{uri} (no fetching rules)." unless @rules.include?(type)
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
      # add ontology type
      add_type(uri, type, data)
      # add fetching time
      add_provenance(uri, data)
      # yield graph of the fetched entity
      yield filter(data, uri)
    end
  end

end
