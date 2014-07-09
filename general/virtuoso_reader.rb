#!/usr/bin/env ruby
require 'yaml'
require 'sparql/client'
require 'rdf'
require 'rdf/virtuoso'
require 'logger'

class VirtuosoReader
  def initialize
    @log = Logger.new('log', 'daily')
    @repo = RDF::Virtuoso::Repository.new('http://localhost:8890/sparql',
                                           update_uri: 'http://localhost:8890/sparql-auth',
                                           username: secrets['databases']['virtuoso']['username'],
                                           password: secrets['databases']['virtuoso']['password'],
                                           auth_method: 'digest')
  end

  def set_graph(graph)
    @graph = graphs[graph.to_s]
  end

  def get_predicates_and_objects_for(subject: , graph: @graph, filter: [])
    graph = RDF::URI.new(graph)
    subject = RDF::URI.new(subject)

    begin
      query = RDF::Virtuoso::Query.select.where([subject, :p, :o]).filters(filter).graph(graph)
      @repo.select(query)
    rescue Exception => e
      @log.error e
    end
  end

  def get_subjects_for(predicate: , object: , graph: @graph)
    graph = RDF::URI.new(graph)
    predicate = RDF::URI.new(predicate)
    object = RDF::URI.new(object)

    begin
      query = RDF::Virtuoso::Query.select.where([:s, predicate, object]).graph(graph)
      result = @repo.select(query)
      result.bindings[:s]
    rescue Exception => e
      @log.error e
    end
  end

  def get_objects_for(subject: , predicate: , graph: @graph)
    graph = RDF::URI.new(graph)
    subject = RDF::URI.new(subject)
    predicate = RDF::URI.new(predicate)

    begin
      query = RDF::Virtuoso::Query.select.where([subject, predicate, :o]).graph(graph)
      result = @repo.select(query)
      result.bindings[:o]
    rescue Exception => e
      @log.error e
    end
  end

  def get_values_for(subject: , graph: @graph)
    graph = RDF::URI.new(graph)
    subject = RDF::URI.new(subject)

    begin
      query = RDF::Virtuoso::Query.select.where([subject, :p, :o]).graph(graph)
      result = @repo.select(query)
      yield result if block_given?
      result
    rescue Exception => e
      @log.error e
    end
  end

  private
  def secrets
    @secrets ||= YAML.load_file '../config/secrets.yml'
  end

  def graphs
    @graphs ||= YAML.load_file('../config/namespaces.yml')['graphs']
  end
end