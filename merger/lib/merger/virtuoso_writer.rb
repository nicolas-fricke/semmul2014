#!/usr/bin/env ruby
require 'yaml'
require 'sparql/client'
require 'rdf'
require 'rdf/virtuoso'

class Merger::VirtuosoWriter
  def initialize
    @repo = RDF::Virtuoso::Repository.new('http://localhost:8890/sparql',
                                           update_uri: 'http://localhost:8890/sparql-auth',
                                           username: Merger::Config.secrets['databases']['virtuoso']['username'],
                                           password: Merger::Config.secrets['databases']['virtuoso']['password'],
                                           auth_method: 'digest')
  end

  def new_triple(subject, predicate, object, graph = Merger::Config.namespaces['graphs']['merged'], literal: true)
    graph = RDF::URI.new(graph)
    subject = RDF::URI.new(subject)
    predicate = RDF::URI.new(predicate)
    object = RDF::URI.new(object) unless literal

    query = RDF::Virtuoso::Query.insert([subject, predicate, object]).graph(graph)
    p @repo.insert(query)
  end

  private
  def secrets
    @secrets ||= YAML.load_file 'config/secrets.yml'
  end
end