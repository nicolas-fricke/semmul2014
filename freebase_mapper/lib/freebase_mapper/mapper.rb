require 'yaml'
require 'json'

class FreebaseMapper::Mapper
  BASE_NAMESPACE    = 'http://www.hpi.uni-potsdam.de/semmul2014/mapped/tmdb/'
  LOM_NAMESPACE     = 'http://www.hpi.uni-potsdam.de/semmul2014/lodofmovies.owl#'
  SCHEMA_NAMESPACE  = 'http://schema.org/'
  DBPEDIA_NAMESPACE = 'http://dbpedia.org/ontology/'
  RDF_NAMESPACE     = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
  RDFS_NAMESPACE    = 'http://www.w3.org/2000/01/rdf-schema#'
  XSD_NAMESPACE     = 'http://www.w3.org/2001/XMLSchema#'
  FREEBASE_NS       = 'http://rdf.freebase.com/ns'

  def initialize
    @virtuoso_writer = FreebaseMapper::VirtuosoWriter.new
    @virtuoso_reader = FreebaseMapper::VirtuosoReader.new
    @receiver = FreebaseMapper::MsgConsumer.new

    puts "listening on queue #{@receiver.queue_name :movie_uri}"
    @receiver.subscribe(type: :movie_uri) { |movie_uri| map movie_uri }
    #map 'http://rdf.freebase.com/ns/m/05gdbn'
  end

  def map(raw_db_uri)
    p "mapping #{raw_db_uri}"
    @virtuoso_writer.new_triple raw_db_uri, "#{RDF_NAMESPACE}type", "#{LOM_NAMESPACE}Movie"

    map_title raw_db_uri
    map_description raw_db_uri
    map_release_dates raw_db_uri
    map_production_companies raw_db_uri
    map_cast raw_db_uri
    map_director raw_db_uri



    #/common/topic/official_website
    #/film/film/film_format
    #/film/film/metacritic_id
    #/film/film/netflix_id
    #/film/film/rottentomatoes_id
    #/film/film/subjects
    #/film/film/traileraddict_id
    #/film/film/trailers
    #/media_common/netflix_title/netflix_genres
    #/film/film/prequel
    #/film/film/sequel
    #/film/film/runtime
    #/film/film_cut/runtime
    #/film/film/language
    #/language/human_language/iso_639_1_code
    #/film/film/country
    #/film/film/genre
    #/film/film/soundtrack
    #/film/film/distributors
    #/film/film_film_distributor_relationship/distributor
    #/film/film/other_crew
    #/film/film_crew_gig/crewmember
    #/film/film_crew_gig/film_crew_role
    #/film/film/cinematography
    #/film/film/costume_design_by
    #/film/film/edited_by
    #/film/film/executive_produced_by
    #/film/film/film_art_direction_by
    #/film/film/film_casting_director
    #/film/film/film_production_design_by
    #/film/film/music
    #/film/film/produced_by
    #/film/film/story_by
    #/film/film/written_by




    p 'done'
  end

  def map_title(raw_db_uri)
    titles = @virtuoso_reader.get_objects_for subject: raw_db_uri,
                                              predicate: "#{FREEBASE_NS}/type/object/name"
    titles.each do |title|
      @virtuoso_writer.new_triple raw_db_uri,
                                  "#{SCHEMA_NAMESPACE}name",
                                  title
    end if titles
  end

  def map_description(raw_db_uri)
    descriptions = @virtuoso_reader.get_objects_for subject: raw_db_uri,
                                                    predicate: "#{FREEBASE_NS}/common/topic/description"
    descriptions.each do |description|
      @virtuoso_writer.new_triple raw_db_uri,
                                  "#{SCHEMA_NAMESPACE}description",
                                  description
    end if descriptions
  end

  def map_release_dates(raw_db_uri)
    dates = @virtuoso_reader.get_objects_for subject: raw_db_uri,
                                             predicate: "#{FREEBASE_NS}/film/film/initial_release_date"
    dates.each do |release_date|
      begin
        @virtuoso_writer.new_triple raw_db_uri,
                                    "#{SCHEMA_NAMESPACE}datePublished",
                                    release_date
      rescue ArgumentError
        puts "Could not parse release date `#{release_date.to_s}' as date."
      end
    end if dates
  end

  def map_production_companies(raw_db_uri)
    companies = @virtuoso_reader.get_objects_for subject: raw_db_uri,
                                                predicate: "#{FREEBASE_NS}/film/film/production_companies"

    companies.each do |company_uri|
      @virtuoso_writer.new_triple raw_db_uri,
                                  "#{SCHEMA_NAMESPACE}productionCompany",
                                  company_uri,
                                  literal: false
      @virtuoso_writer.new_triple company_uri,
                                  "#{RDF_NAMESPACE}type",
                                  "#{SCHEMA_NAMESPACE}Organization",
                                  literal: false

      company_names = @virtuoso_reader.get_objects_for subject: company_uri,
                                                      predicate: "#{FREEBASE_NS}/type/object/name"
      company_names.each do |company_name|
        @virtuoso_writer.new_triple company_uri,
        "#{SCHEMA_NAMESPACE}name",
        company_name

      end if company_names
    end if companies
  end

  def map_director(raw_db_uri)
    directors = @virtuoso_reader.get_objects_for subject: raw_db_uri,
                                                 predicate: "#{FREEBASE_NS}/film/film/directed_by"

    directors.each do |director_uri|
      @virtuoso_writer.new_triple raw_db_uri,
                                  "#{SCHEMA_NAMESPACE}director",
                                  director_uri,
                                  literal: false
      @virtuoso_writer.new_triple director_uri,
                                  "#{RDF_NAMESPACE}type",
                                  "#{LOM_NAMESPACE}Director",
                                  literal: false

      director_names = @virtuoso_reader.get_objects_for subject: director_uri,
                                                       predicate: "#{FREEBASE_NS}/type/object/name"
      director_names.each do |director_name|
        @virtuoso_writer.new_triple director_uri,
                                    "#{SCHEMA_NAMESPACE}name",
                                    director_name

      end if director_names
    end if directors
  end

  def map_cast(raw_db_uri)
    performances = @virtuoso_reader.get_objects_for subject: raw_db_uri,
                                                    predicate: "#{FREEBASE_NS}/film/film/starring"
    performances.each do |performance_uri|
      @virtuoso_writer.new_triple raw_db_uri,
                                  "#{LOM_NAMESPACE}performance",
                                  performance_uri
      @virtuoso_writer.new_triple performance_uri,
                                  "#{RDF_NAMESPACE}type",
                                  "#{LOM_NAMESPACE}Performance",
                                  literal: false

      characters = @virtuoso_reader.get_objects_for subject: performance_uri,
                                                    predicate: "#{FREEBASE_NS}/film/performance/character"
      characters.each do |character_uri|
        @virtuoso_writer.new_triple performance_uri,
                                    "#{LOM_NAMESPACE}character",
                                    character_uri
        @virtuoso_writer.new_triple character_uri,
                                    "#{RDF_NAMESPACE}type",
                                    "#{DBPEDIA_NAMESPACE}FictionalCharacter",
                                    literal: false


        character_names = @virtuoso_reader.get_objects_for subject: character_uri,
                                                         predicate: "#{FREEBASE_NS}/type/object/name"
        character_names.each do |character_name|
          @virtuoso_writer.new_triple character_uri,
                                      "#{SCHEMA_NAMESPACE}name",
                                      character_name
        end if character_names
      end if characters


      actors = @virtuoso_reader.get_objects_for subject: performance_uri,
                                                predicate: "#{FREEBASE_NS}/film/performance/actor"
      actors.each do |actor_uri|
        @virtuoso_writer.new_triple performance_uri,
                                    "#{LOM_NAMESPACE}actor",
                                    actor_uri
        @virtuoso_writer.new_triple actor_uri,
                                    "#{RDF_NAMESPACE}type",
                                    "#{DBPEDIA_NAMESPACE}Actor",
                                    literal: false
        actor_names = @virtuoso_reader.get_objects_for subject: actor_uri,
                                                       predicate: "#{FREEBASE_NS}/type/object/name"
        actor_names.each do |actor_name|
          @virtuoso_writer.new_triple actor_uri,
                                      "#{SCHEMA_NAMESPACE}name",
                                      actor_name
        end if actor_names
      end if actors


    end if performances
  end



end
