require "linkeddata"

# A DBpediaCrawler is a crawler which crawls a DBpedia. :)
class DbpediaCrawler

  #
  # instance creation
  #

  # path of the folder with the queries, ending with "/"
  QUERIES_PATH = File::expand_path("../queries", __FILE__) + "/"
  # file extension of files with queries
  QUERIES_FILE_EXT = ".txt"

  # symbols which denote queries
  QUERIES = [
    # query the IRIs of relevant entities
    :all_ids
  ]

  DEFAULT_OPTIONS = {
    # URL of the SPARQL endpoint
    endpoint: "http://dbpedia.org/sparql",
    # timeout in milli seconds
    timeout: 666666,
    # MIME type of queried data
    mime: "text/turtle"
  }

  def initialize(options={})
    # use given options or default options
    options = DEFAULT_OPTIONS.merge options
    # apply options
    @endpoint = options[:endpoint]
    @timeout = options[:timeout]
    @mime = options[:mime]
    # load query strings
    initialize_queries
  end

  # load query strings from files
  def initialize_queries
    @queries = {}
    QUERIES.each do |symbol|
      File.open(QUERIES_PATH + symbol.to_s + QUERIES_FILE_EXT, "r") do |file|
        @queries[symbol] = file.read
      end
    end
  end

  #
  # querying
  #

  # Get the data of a specific entity as RDF::Graph
  def linked_data(uri)
    RDF::Graph.load(uri)
  end

  # Get the query result as Array<RDF::Query::Solution>
  def query(query)
    SPARQL::Client.new(@endpoint).query(query)
  end

  # Query all ids
  def query_all_ids()
    query(@queries[:all_ids]).map do |solution|
      solution[:movie]
    end
  end

  #
  # output
  #

  # Get a string representation of a given set of triples 
  # (graph) using this crawler's MIME type
  def stringify(data)
    RDF::Writer.for(content_type: @mime).dump data
  end

  # Get the triples for the given URI as a string,
  # using this crawler's MIME type
  def triples_of_entity(uri)
    stringify linked_data(uri)
  end

end

