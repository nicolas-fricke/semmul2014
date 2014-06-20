# encoding: utf-8

require "linkeddata"

# Class Writer allows write access to a triple store.
class DBpediaCrawler::Writer

private

  # Create SPARQL code to insert the given triples in the triple store.
  #
  # To avoid an error like 
  #   "Virtuoso 37000 Error SP031: SPARQL: Internal error: 
  #    The length of generated SQL text has exceeded 10000 lines of code"
  # for very large sets of triples, multiple queries may be created,
  # all of which insert a part of the data.
  #
  # This is a workaround. Actually, the following should work:
  #   @client.insert_data(data, graph: @graph)
  # but it does not, probably because InsertData#to_s is flawed.
  #
  #   result: array (of query strings)
  def update_queries(data)
    # split up the graph
    batches = split_graph data

    # create queries
    queries = batches.map do |batch|
      update_query batch
    end

    return queries
  end

  # Split up a graph according to maximum batch size.
  #   data: RDF::Graph
  #   result: array of RDF::Graphs
  def split_graph(data)
    batches, batch, count = [], RDF::Graph.new, 0

    data.statements.each do |statement|
      if count >= @config["batch_size"]
        batches << batch
        batch, count = RDF::Graph.new, 0
      end
      batch << statement
      count += 1
    end
    batches << batch if count > 0

    return batches
  end

  # Create a SPARQL update query for the given data.
  #   data: RDF::Graph
  #   result: string
  def update_query(data)
    query = "INSERT DATA INTO <" + @config["graph"] + "> {"
    query += RDF::NTriples::Writer.buffer({validate: false}) do |writer|
      writer << data
    end
    query += " }"
    return query
  end

  # Shorten the message of Errors which include full query strings
  #   message: string
  #   result: string
  def squeeze_message(message)
    return "#{message.lines.first}..."
  end

public

  # Create a new Writer
  #   configuration: hash
  def initialize(configuration)
    @config = configuration
    @client = SPARQL::Client.new @config["endpoint"]
  end

  # Insert all statements in the given graph into this Writer's graph of 
  # this Writer's data store.
  #   data: RDF::Graph
  #   raises: StandardError if update fails
  def insert(data)
    begin
      # get update queries
      queries = update_queries data
      # apply updates
      puts "Writing " + queries.size.to_s + " batch(es) of data..."
      queries.each do |query_string|
        @client.query query_string
      end
    rescue SPARQL::Client::ClientError => e
      puts "# Error while updating triple store: Client error:"
      puts squeeze_message e.message
      raise "insert failed (client error)"
    rescue SPARQL::Client::ServerError => e
      puts "# Error while updating triple store: Server error:"
      puts squeeze_message e.message
      raise "insert failed (server error)"
    rescue StandardError => e
      puts "# Error while updating triple store: " + e.message
      raise "insert failed"
    end
  end

end
