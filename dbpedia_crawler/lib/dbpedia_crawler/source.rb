# encoding: utf-8

require "linkeddata"

# Class Source provides access to a DBpedia.
class DBpediaCrawler::Source

private

  # Execute a given block and return its value. If a StandardError occurs,
  # retry as configured, then raise a StandardError "query failed".
  def execute_with_retries
    retries = @config["query_retries"]
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

  # Helper for printing a message like
  # "Querying results 10000 - 19999...",
  # while handling a paginated query.
  def print_query_page_message(count, limit, offset)
    first_string = offset.to_s.rjust(count.to_s.length)
    last_string = [count - 1, offset + limit - 1].min.to_s.rjust(count.to_s.length)
    puts "Querying results " + first_string + " to " + last_string + "..."
  end

public

  # Create a new Source
  #   configuration: hash
  def initialize(configuration)
    @config = configuration
    @client = SPARQL::Client.new @config["endpoint"]
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

  # Execute a given SPARQL query using pagination.
  #   query: query string; the result set must have a variable ?result;
  #          the query must include "<<limit>>" and "<<offset>>" to be
  #          substituted
  #   count_query: query string of the query used to get the result size
  #   result: accumulated query result
  def query_with_pagination(query, count_query)
    # get the number of URIs
    count = query(count_query)[0][:result].to_s.to_i
    puts "Result size: " + count.to_s
    # query with pages
    result = []
    (0..(count / @config["page_size"]).floor).each do |page_number|
      # get copy of the query string and apply parameters
      query_string = query.clone
      query_string["<<limit>>"]= @config["page_size"].to_s
      query_string["<<offset>>"]= (page_number * @config["page_size"]).to_s
      # query and append results
      print_query_page_message(count, @config["page_size"], page_number * @config["page_size"])
      result.concat(query(query_string).map { |solution| solution[:result].to_s })
    end
    return result
  end

end
