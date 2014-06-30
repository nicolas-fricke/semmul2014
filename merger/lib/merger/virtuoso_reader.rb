#!/usr/bin/env ruby
require 'yaml'
require 'sparql/client'
require 'rdf'
require 'rdf/virtuoso'

class Merger::VirtuosoReader
  def initialize
    @repo = RDF::Virtuoso::Repository.new('http://localhost:8890/sparql',
                                           update_uri: 'http://localhost:8890/sparql-auth',
                                           username: Merger::Config.secrets['databases']['virtuoso']['username'],
                                           password: Merger::Config.secrets['databases']['virtuoso']['password'],
                                           auth_method: 'digest')
  end

  def get_objects_for(subject: , predicate: , graph: Merger::Config.namespaces['graphs']['mapped'])
    graph = RDF::URI.new(graph)
    subject = RDF::URI.new(subject)
    predicate = RDF::URI.new(predicate)

    query = RDF::Virtuoso::Query.select.where([subject, predicate, :o]).graph(graph)
    result = @repo.select(query)
    # puts result.bindings
    # TODO get value from bindings or find other way to get variables/values
    # goal: array with result values
    result.bindings[:o]
  end
end