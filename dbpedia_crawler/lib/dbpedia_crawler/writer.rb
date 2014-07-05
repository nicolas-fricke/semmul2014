# encoding: utf-8

require "linkeddata"

# Class Writer allows write access to a triple store.
class DBpediaCrawler::Writer

private

  # Split up a graph according to maximum batch size.
  # data: RDF::Graph
  # result: array of RDF::Graphs
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

  # Delete all triples whose subject or object is the given entity.
  #   entity: RDF::URI
  def delete_triples_for(entity)
    # delete triples with entity as subject
    @virtuoso.delete_triple(subject: entity.to_s)
  end

public

  # Create a new Writer
  #   configuration: hash
  def initialize(configuration)
    @config = configuration

    @virtuoso = VirtuosoWriter.new
    @virtuoso.set_graph 'raw'
  end

  # Insert all statements in the given graph into this Writer's graph of 
  # this Writer's data store. Triples previously inserted for the given
  # entity are deleted before the update.
  #   uri: the entity for which triples are inserted
  #   data: RDF::Graph
  #   raises: StandardError if update fails
  def update(uri, data)
    begin
      # delete previous triples
      delete_triples_for uri
      # get update queries
      batches = split_graph data
      # apply updates
      puts "Writing #{batches.size} batch(es) of data..."
      batches.each do |batch|
        @virtuoso.write_triples batch
      end
    rescue StandardError => e
      puts "# Error while updating triple store: #{e.message}"
      raise "insert failed"
    end
  end

end
