require "linkeddata"

module DBpediaCrawler

  # Class Source provides access to a DBpedia.
  #
  # Use Source.query to execute a SPARQL query.
  # Use Source::triples_for for HTTP access to linked data.
  # TODO: incorporate timeout (see Client.new options)
  #
  class Source

    # Create a new Source
    #   configuration: hash
    def initialize(configuration)
      @client = SPARQL::Client.new configuration["endpoint"]
    end

    # Send a SPARQL query
    #   query: string
    #   result: Array<RDF::Query::Solution>
    def query(query)
      @client.query query
    end

    # Get linked data for the given URI via HTTP
    #   uri: string
    #   result: RDF::Graph
    def Source::triples_for(uri)
      RDF::Graph.load uri
    end

  end

end
