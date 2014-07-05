module FreebaseMapper
  require_relative '../../general/msg_consumer'
  require_relative '../../general/msg_publisher'
  require_relative '../../general/virtuoso_writer'
  require_relative '../../general/virtuoso_reader'

  require 'yaml'
  require 'json'

  class FreebaseMapper::Mapper

    def initialize
      @receiver = MsgConsumer.new
      @receiver.set_queue 'raw_freebase'

      @publisher = MsgPublisher.new
      @publisher.set_queue 'mapping'

      @virtuoso_writer = VirtuosoWriter.new
      @virtuoso_writer.set_graph 'mapped'

      @virtuoso_reader = VirtuosoReader.new
      @virtuoso_reader.set_graph 'raw'

      puts "listening on queue #{@receiver.queue_name :movie_uri}"
      @receiver.subscribe(type: :movie_uri) { |movie_uri| map movie_uri }
      #map 'http://rdf.freebase.com/ns/m/09zb16'
    end

    def map(raw_db_uri)
      start_time = Time.now
      p "mapping #{raw_db_uri}"
      @virtuoso_writer.new_triple raw_db_uri, "#{schemas['rdf']}type", "#{schemas['lom']}Movie"

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



      @virtuoso_writer.new_triple raw_db_uri,
                                  @schemas['pav_lastupdateon'],
                                  set_xsd_type(DateTime.now, 'dateTime')
      @publisher.enqueue :movie_uri, raw_db_uri
      p "Finished within #{Time.now - start_time}s, writing to #{@publisher.queue_name :movie_uri}"
    end

    def map_title(raw_db_uri)
      titles = @virtuoso_reader.get_objects_for subject: raw_db_uri,
                                                predicate: "#{schemas['base_freebase']}/type/object/name"
      titles.each do |title|
        @virtuoso_writer.new_triple raw_db_uri,
                                    "#{schemas['schema']}name",
                                    title
      end if titles
    end

    def map_description(raw_db_uri)
      descriptions = @virtuoso_reader.get_objects_for subject: raw_db_uri,
                                                      predicate: "#{schemas['base_freebase']}/common/topic/description"
      descriptions.each do |description|
        @virtuoso_writer.new_triple raw_db_uri,
                                    "#{schemas['schema']}description",
                                    description
      end if descriptions
    end

    def map_release_dates(raw_db_uri)
      dates = @virtuoso_reader.get_objects_for subject: raw_db_uri,
                                               predicate: "#{schemas['base_freebase']}/film/film/initial_release_date"
      dates.each do |release_date|
        begin
          @virtuoso_writer.new_triple raw_db_uri,
                                      "#{schemas['schema']}datePublished",
                                      release_date
        rescue ArgumentError
          puts "Could not parse release date `#{release_date.to_s}' as date."
        end
      end if dates
    end

    def map_production_companies(raw_db_uri)
      companies = @virtuoso_reader.get_objects_for subject: raw_db_uri,
                                                   predicate: "#{schemas['base_freebase']}/film/film/production_companies"

      companies.each do |company_uri|
        @virtuoso_writer.new_triple raw_db_uri,
                                    "#{schemas['schema']}productionCompany",
                                    company_uri,
                                    literal: false
        @virtuoso_writer.new_triple company_uri,
                                    "#{schemas['rdf']}type",
                                    "#{schemas['schema']}Organization",
                                    literal: false

        company_names = @virtuoso_reader.get_objects_for subject: company_uri,
                                                         predicate: "#{schemas['base_freebase']}/type/object/name"
        company_names.each do |company_name|
          @virtuoso_writer.new_triple company_uri,
                                      "#{schemas['schema']}name",
                                      company_name

        end if company_names
      end if companies
    end

    def map_director(raw_db_uri)
      directors = @virtuoso_reader.get_objects_for subject: raw_db_uri,
                                                   predicate: "#{schemas['base_freebase']}/film/film/directed_by"

      directors.each do |director_uri|
        @virtuoso_writer.new_triple raw_db_uri,
                                    "#{schemas['schema']}director",
                                    director_uri,
                                    literal: false
        @virtuoso_writer.new_triple director_uri,
                                    "#{schemas['rdf']}type",
                                    "#{schemas['lom']}Director",
                                    literal: false

        director_names = @virtuoso_reader.get_objects_for subject: director_uri,
                                                          predicate: "#{schemas['base_freebase']}/type/object/name"
        director_names.each do |director_name|
          @virtuoso_writer.new_triple director_uri,
                                      "#{schemas['schema']}name",
                                      director_name

        end if director_names

        director_aliases = @virtuoso_reader.get_objects_for subject: director_uri,
                                                         predicate: "#{schemas['base_freebase']}/common/topic/alias"
        director_aliases.each do |director_alias|
          @virtuoso_writer.new_triple director_uri,
                                      "#{schemas['schema']}alternateName",
                                      director_alias
        end if director_aliases

        director_birthdates = @virtuoso_reader.get_objects_for subject: director_uri,
                                                            predicate: "#{schemas['base_freebase']}/people/person/date_of_birth"
        director_birthdates.each do |director_birthdate|
          @virtuoso_writer.new_triple director_uri,
                                      "#{schemas['schema']}birthDate",
                                      director_birthdate
        end if director_birthdates
      end if directors
    end

    def map_cast(raw_db_uri)
      performances = @virtuoso_reader.get_objects_for subject: raw_db_uri,
                                                      predicate: "#{schemas['base_freebase']}/film/film/starring"
      performances.each do |performance_uri|
        @virtuoso_writer.new_triple raw_db_uri,
                                    "#{schemas['lom']}performance",
                                    performance_uri
        @virtuoso_writer.new_triple performance_uri,
                                    "#{schemas['rdf']}type",
                                    "#{schemas['lom']}Performance",
                                    literal: false

        characters = @virtuoso_reader.get_objects_for subject: performance_uri,
                                                      predicate: "#{schemas['base_freebase']}/film/performance/character"
        characters.each do |character_uri|
          @virtuoso_writer.new_triple performance_uri,
                                      "#{schemas['lom']}character",
                                      character_uri
          @virtuoso_writer.new_triple character_uri,
                                      "#{schemas['rdf']}type",
                                      "#{schemas['dbpedia']}FictionalCharacter",
                                      literal: false


          character_names = @virtuoso_reader.get_objects_for subject: character_uri,
                                                             predicate: "#{schemas['base_freebase']}/type/object/name"
          character_names.each do |character_name|
            @virtuoso_writer.new_triple character_uri,
                                        "#{schemas['schema']}name",
                                        character_name
          end if character_names
        end if characters


        actors = @virtuoso_reader.get_objects_for subject: performance_uri,
                                                  predicate: "#{schemas['base_freebase']}/film/performance/actor"
        actors.each do |actor_uri|
          @virtuoso_writer.new_triple performance_uri,
                                      "#{schemas['lom']}actor",
                                      actor_uri
          @virtuoso_writer.new_triple actor_uri,
                                      "#{schemas['rdf']}type",
                                      "#{schemas['dbpedia']}Actor",
                                      literal: false
          actor_names = @virtuoso_reader.get_objects_for subject: actor_uri,
                                                         predicate: "#{schemas['base_freebase']}/type/object/name"
          actor_names.each do |actor_name|
            @virtuoso_writer.new_triple actor_uri,
                                        "#{schemas['schema']}name",
                                        actor_name
          end if actor_names




          actor_aliases = @virtuoso_reader.get_objects_for subject: actor_uri,
                                                         predicate: "#{schemas['base_freebase']}/common/topic/alias"
          actor_aliases.each do |actor_alias|
            @virtuoso_writer.new_triple actor_uri,
                                        "#{schemas['schema']}alternateName",
                                        actor_alias
          end if actor_aliases

          actor_birthdates = @virtuoso_reader.get_objects_for subject: actor_uri,
                                                           predicate: "#{schemas['base_freebase']}/people/person/date_of_birth"
          actor_birthdates.each do |actor_birthdate|
            @virtuoso_writer.new_triple actor_uri,
                                        "#{schemas['schema']}birthDate",
                                        actor_birthdate
          end if actor_birthdates
        end if actors


      end if performances
    end

    private
    def set_xsd_type(literal, type)
      "#{literal}^^#{@schemas['xsd']}#{type}"
    end

    def schemas
      @schemas ||= load_schemas
    end

    def load_schemas
      file = YAML.load_file '../config/namespaces.yml'
      file['schemas']
    end
  end
end
