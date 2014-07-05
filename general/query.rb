# encoding: utf-8

require 'rdf/virtuoso'

#
# This is a quick fix for a bug in RDF::Virtuoso::Query.
# The original implementation of "#insert"
# (see http://rubydoc.info/github/digibib/rdf-virtuoso/frames)
# discards the data types of any RDF::Literal given as argument.
#
# Use a query like
#  select ?s ?p ?o (datatype(?o)) where { 
#    ?s ?p ?o .
#  } 
#  order by ?s
#  limit 1000
# to see triples and data types (for literals) in a triple store.
#
class RDF::Virtuoso::Query

  def insert(*patterns)
    new_patterns = []
    patterns.each do |pattern|
      new_patterns << pattern.map do |value|
        if value.is_a?(Symbol)
          value = RDF::Query::Variable.new(value)
        elsif value.is_a?(RDF::URI) or value.is_a?(RDF::Literal)
          # Added "or value.is_a?(RDF::Literal)" to keep the original "value"
          # (and its data type) if it is a RDF::Literal.
          value = value
        else
          # In the original code, if "value" was a RDF::Literal already,
          # this would have created a new RDF::Literal from it, thus removing
          # the data type (and assigning the default "string" type).
          value = RDF::Literal.new(value)
        end
      end
    end
    @data_values = build_patterns(new_patterns)
    self
  end

end
