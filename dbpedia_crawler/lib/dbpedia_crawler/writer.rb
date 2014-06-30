# encoding: utf-8

require "linkeddata"

# Class Writer allows write access to a triple store.
class DBpediaCrawler::Writer

private

  # Delete all triples whose subject or object is the given entity.
  #   entity: RDF::URI
  def delete_triples_for(entity)
    # delete triples with entity as subject
    @virtuoso.delete_triple(subject: entity.to_s)
  end

public

  # Create a new Writer
  #   configuration: hash
  def initialize
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
      # apply updates
      puts "Writing #{data.count} triples..."
      data.each_statement do |statement|
        s, p, o = statement.subject, statement.predicate, statement.object
        @virtuoso.new_triple(s.to_s, p.to_s, o.to_s, literal: o.literal?)
      end
    rescue StandardError => e
      puts "# Error while updating triple store: #{e.message}"
      raise "insert failed"
    end
  end

end
