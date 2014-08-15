#!/usr/bin/env ruby
require 'yaml'
require 'sparql/client'
require 'rdf'
require 'rdf/virtuoso'
require 'logger'

class VirtuosoReader
  def initialize(graph: nil)
    @log = Logger.new('log', 'daily')
    @repo = RDF::Virtuoso::Repository.new('http://localhost:8890/sparql',
                                           update_uri: 'http://localhost:8890/sparql-auth',
                                           username: secrets['databases']['virtuoso']['username'],
                                           password: secrets['databases']['virtuoso']['password'],
                                           auth_method: 'digest')
    set_graph graph
  end

  def set_graph(graph)
    @graph = graphs[graph.to_s]
  end

  def get_predicates_and_objects_for(subject: , graph: @graph, filter: [])
    subject = RDF::URI.new(subject)

    begin
      query = RDF::Virtuoso::Query.select.where([subject, :p, :o]).filters(filter).to_s
      query.insert query.index('WHERE'),"FROM <#{graph}> "
      results = @repo.select(query)
      hash_of_arrays_to_array_of_hashes results.bindings
    rescue Exception => e
      @log.error e
      nil
    end
  end

  def get_subjects_for(predicate: , object: , graph: @graph)
    predicate = RDF::URI.new(predicate)
    object = RDF::URI.new(object)

    begin
      # for some reason `.graph` is ignored
      query = RDF::Virtuoso::Query.select.where([:s, predicate, object]).to_s
      query.insert query.index('WHERE'),"FROM <#{graph}> "
      result = @repo.select(query)
      result.bindings[:s]
    rescue Exception => e
      @log.error e
      nil
    end
  end

  def exists_subject(subject:, graph: @graph)
    subject = RDF::URI.new("http://semmul2014.hpi.de/tmdb/movie/11449")
    begin
      query = RDF::Virtuoso::Query.select.where([subject, :p, :o]).to_s
      query.insert query.index('WHERE'),"FROM <#{graph}> "
      results = @repo.select(query)

      return results.empty?
    rescue Exception => e
      puts e
      nil
    end
  end

  def get_objects_for(subject: , predicate: , graph: @graph)
    subject = RDF::URI.new(subject)
    predicate = RDF::URI.new(predicate)

    begin
      # for some reason `.graph` is ignored
      query = RDF::Virtuoso::Query.select.where([subject, predicate, :o]).to_s
      query.insert query.index('WHERE'),"FROM <#{graph}> "
      result = @repo.select(query)
      result.bindings[:o]
    rescue Exception => e
      @log.error e
      nil
    end
  end

  def get_values_for(subject: , graph: @graph)
    subject = RDF::URI.new(subject)

    begin
      query = RDF::Virtuoso::Query.select.where([subject, :p, :o]).to_s
      query.insert query.index('WHERE'),"FROM <#{graph}> "
      result = @repo.select(query)
      yield result if block_given?
      result
    rescue Exception => e
      @log.error e
      nil
    end
  end

  private
  def hash_of_arrays_to_array_of_hashes(hash)
    hash.inject([]) do |array, (key,values)|
      values.each_with_index do |value,index|
        (array[index] ||= {})[key] = value
      end
      array
    end
  end

  def secrets
    @secrets ||= YAML.load_file '../config/secrets.yml'
  end

  def graphs
    @graphs ||= YAML.load_file('../config/namespaces.yml')['graphs']
  end
end