#!/usr/bin/env ruby
require 'yaml'
require 'sparql/client'
require 'rdf'
require 'rdf/virtuoso'
require 'logger'

require_relative 'query'

class VirtuosoWriter
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

  def new_triple(subject, predicate, object, graph: @graph, literal: true)
    graph = RDF::URI.new(graph)
    subject = RDF::URI.new(subject)
    predicate = RDF::URI.new(predicate)
    object = RDF::URI.new(object) unless literal

    begin
      query = RDF::Virtuoso::Query.insert([subject, predicate, object]).graph(graph)
      p @repo.insert(query)
    rescue Exception => e
      @log.error e
    end
  end

  # Writes all triples in a given RDF::Enumerable to the store
  def write_triples(enumerable, graph: @graph)
    patterns = []
    enumerable.each_triple do |subject, predicate, object|
      patterns << [subject, predicate, object]
    end

    begin
      query = RDF::Virtuoso::Query.insert(*patterns).graph(graph)
      puts query
      p @repo.insert_data(query)
    rescue Exception => e
      @log.error e
    end
  end

  def delete_triple(subject: :s, predicate: :p, object: :o, graph: @graph)
    graph = RDF::URI.new(graph)
    subject = RDF::URI.new(subject) unless subject.eql? :s
    predicate = RDF::URI.new(predicate) unless predicate.eql? :p
    object = RDF::URI.new(object) unless object.eql? :o

    begin
      query = RDF::Virtuoso::Query.delete([subject, predicate, object]).graph(graph).where([subject, predicate, object])
      p @repo.insert(query)
    rescue Exception => e
      @log.error e
    end
  end

  private
  def secrets
    @secrets ||= YAML.load_file '../config/secrets.yml'
  end

  def graphs
    file  ||= YAML.load_file '../config/namespaces.yml'
    @graphs = file['graphs']
  end
end
