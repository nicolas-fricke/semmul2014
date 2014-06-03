require "linkeddata"

module DBpediaCrawler

  # Class Writer allows write access to a triple store.
  class Writer

    # Create a new Writer
    #   configuration: hash
    def initialize(configuration)
      @client = SPARQL::Client.new configuration["endpoint"]
      @graph = configuration["graph"]
    end

    # Insert all statements in the given graph into this Writer's graph of 
    # this Writer's data store.
    #   data: RDF::Graph
    def insert(data)
      # The following should work:
      #   @client.insert_data(data, graph: @graph)
      # but does not, probably because InsertData#to_s is wrong.
      query = "INSERT DATA INTO <" + @graph + "> {"
      query += RDF::NTriples::Writer.buffer({validate: false}) do |writer|
        writer << data
      end
      query += " }"
      puts query
      @client.query query
    end

  end

end
