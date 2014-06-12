# encoding: utf-8

require "linkeddata"

module DBpediaCrawler

  # Class Source provides access to a DBpedia.
  #
  # Use Source.query to execute a SPARQL query.
  # Use Source::triples_for for HTTP access to linked data.
  class Source

  private

    # Execute a given block and return its value. If a StandardError occurs,
    # retry as configured, then raise a StandardError "query failed".
    def execute_with_retries
      retries = @query_retries
      loop do
        begin
          return yield
        rescue StandardError => e
          puts "# Error while querying (retries: " + retries.to_s + "):"
          if retries > 0
            puts e.message
            retries -= 1
          else
            puts e.message, e.backtrace
            raise "query failed"
          end
        end
      end
    end

  public

    # Create a new Source
    #   configuration: hash
    def initialize(configuration)
      @client = SPARQL::Client.new configuration["endpoint"]
      @query_retries = configuration["query_retries"]
    end

    # Execute a SPARQL query
    #   query: string
    #   result: Array<RDF::Query::Solution>
    #   raises: StandardError if querying fails after retries
    def query(query)
      execute_with_retries { return @client.query query }
    end

    # Get linked data for the given URI via HTTP
    #   uri: string
    #   result: RDF::Graph
    #   raises: StandardError if querying fails after retries
    def triples_for(uri)
      execute_with_retries { return RDF::Graph.load uri }
    end

  end

end
