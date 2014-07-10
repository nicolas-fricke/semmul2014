module FreebaseUpdater
  require_relative '../../freebase_crawler/lib/freebase_crawler'
  require_relative '../../general/msg_consumer'
  require_relative '../../general/virtuoso_writer'
  require_relative '../../general/msg_publisher'
  require 'yaml'
  require 'json'
  require 'rdf'

  class FreebaseUpdater::Updater

    def initialize
      # ========== settings ==========
      @verbose = false
      @demo = false
      # ==============================

      @crawler = FreebaseCrawler::Crawler.new

      @virtuoso_writer = VirtuosoWriter.new verbose: @verbose
      @virtuoso_writer.set_graph 'raw'

      @receiver = MsgConsumer.new
      @receiver.set_queue 'source_freebase'

      @publisher = MsgPublisher.new
      @publisher.set_queue 'raw_freebase'

      @log = Logger.new('log', 'daily')

      if @demo
        %w(
          /m/08phg9
          /m/0hhqv27
          /m/04j1zjw
          /m/07f_t4
          /m/0dtfn
          /m/0cc7hmk
          /m/0gtxbqr
          /m/06zkfsy
          /m/0lq6fb5
          /m/05jzt3
          /m/02ktj7
          /m/02dr9j
          ).each { |movie_id| update movie_id }
      else
        puts "listening on queue #{@receiver.queue_name :movie_id}"
        @receiver.subscribe(type: :movie_id) { |movie_id| update(movie_id) }
      end
    end

    def update(topic_id)
      begin
        p "Looking up MID #{topic_id} ..."
        start_time = Time.now

        retrieve_topic topic_id do |topic_description|
          movie_uri = schemas['base_freebase'] + topic_id

          # try to delete existing triples for movie first
          @virtuoso_writer.delete_triple subject: movie_uri

          @virtuoso_writer.new_triple movie_uri,
                                      schemas['base_freebase']+'/type/object/mid',
                                      topic_id

          update_primitives topic_description, topic_id

          # resources
          update_persons topic_description, topic_id
          update_language topic_description, topic_id
          update_country topic_description, topic_id
          update_genres topic_description, topic_id
          update_production_companies topic_description, topic_id
          update_soundtrack topic_description, topic_id

          #nested resources
          update_cast topic_description, topic_id
          update_runtime topic_description, topic_id
          update_distributors topic_description, topic_id
          update_crew topic_description, topic_id

          update_pav movie_uri

          p "Finished within #{Time.now - start_time}s, writing to #{@publisher.queue_name :movie_uri}"
          @publisher.enqueue :movie_uri, movie_uri
        end
      rescue => e
        p e
        @log.error e
      end
    end

    def update_primitives(topic_description, topic_id)
      # strings
      %W( /common/topic/official_website
        /film/film/film_format
        /film/film/metacritic_id
        /film/film/netflix_id
        /film/film/rottentomatoes_id
        /film/film/subjects
        /film/film/traileraddict_id
        /film/film/trailers
        /media_common/netflix_title/netflix_genres
        /type/object/name).each do |locator|
        retrieve_text topic_description, topic_id, locator
      end

      retrieve_value topic_description, topic_id, '/common/topic/description'

      # dates
      retrieve_date topic_description, topic_id, '/film/film/initial_release_date'

      # other objects
      retrieve_uri topic_description, topic_id, '/type/object/type'
      # these movies should be fetched automatically
      retrieve_uri topic_description, topic_id, '/film/film/prequel'
      retrieve_uri topic_description, topic_id, '/film/film/sequel'
    end

    def update_runtime(topic_description, topic_id)
      runtimes = topic_description['/film/film/runtime']
      if runtimes
        runtimes['values'].each do |runtime|
          # TODO think of a better solution here
          runtime['property']['/film/film_cut/runtime']['values'].each do |cut|
            @virtuoso_writer.new_triple schemas['base_freebase']+topic_id,
                                        schemas['base_freebase']+'/film/film_cut/runtime',
                                        RDF::Literal.new(cut['value'])
          end
        end
      end
    end

    def update_language(topic_description, topic_id)
      languages = topic_description['/film/film/language']
      if languages
        languages['values'].each do |language|
          @virtuoso_writer.delete_triple subject: schemas['base_freebase']+language['id']

          @virtuoso_writer.new_triple schemas['base_freebase']+topic_id,
                                      schemas['base_freebase']+'/film/film/language',
                                      schemas['base_freebase']+language['id'],
                                      literal: false

          @virtuoso_writer.new_triple schemas['base_freebase']+language['id'],
                                      schemas['base_freebase']+'/type/object/name',
                                      language['text']

          @virtuoso_writer.new_triple schemas['base_freebase']+language['id'],
                                      schemas['base_freebase']+'/language/human_language/iso_639_1_code',
                                      language['lang']

          @virtuoso_writer.new_triple schemas['base_freebase']+language['id'],
                                      schemas['base_freebase']+'type/object/type',
                                      schemas['base_freebase']+'/language/human_language',
                                      literal: false

          update_pav schemas['base_freebase']+language['id']
        end
      end
    end

    def update_country(topic_description, topic_id)
      countries = topic_description['/film/film/country']
      if countries
        countries['values'].each do |country|
          country_uri = schemas['base_freebase']+country['id']

          @virtuoso_writer.delete_triple subject: country_uri

          @virtuoso_writer.new_triple schemas['base_freebase']+topic_id,
                                      schemas['base_freebase']+'/film/film/country',
                                      country_uri,
                                      literal: false

          @virtuoso_writer.new_triple country_uri,
                                      schemas['base_freebase']+'/type/object/name',
                                      country['text']

          @virtuoso_writer.new_triple country_uri,
                                      schemas['base_freebase']+'type/object/type',
                                      schemas['base_freebase']+'/location/country',
                                      literal: false

          update_pav country_uri
        end
      end
    end

    def update_genres(topic_description, topic_id)
      genres= topic_description['/film/film/genre']
      if genres
        genres['values'].each do |genre|
          genre_uri = schemas['base_freebase']+genre['id']

          @virtuoso_writer.delete_triple subject: genre_uri

          @virtuoso_writer.new_triple schemas['base_freebase']+topic_id,
                                      schemas['base_freebase']+'/film/film/genre',
                                      genre_uri,
                                      literal: false

          @virtuoso_writer.new_triple genre_uri,
                                      schemas['base_freebase']+'/type/object/name',
                                      genre['text']

          @virtuoso_writer.new_triple genre_uri,
                                      schemas['base_freebase']+'type/object/type',
                                      schemas['base_freebase']+'/film/film_genre',
                                      literal: false

          update_pav genre_uri
        end
      end
    end

    def update_soundtrack(topic_description, topic_id)
      soundtracks= topic_description['/film/film/soundtrack']
      if soundtracks
        soundtracks['values'].each do |soundtrack|
          soundtrack_uri = schemas['base_freebase']+soundtrack['id']

          @virtuoso_writer.delete_triple subject: soundtrack_uri

          @virtuoso_writer.new_triple schemas['base_freebase']+topic_id,
                                      schemas['base_freebase']+'/film/film/soundtrack',
                                      soundtrack_uri,
                                      literal: false

          @virtuoso_writer.new_triple soundtrack_uri,
                                      schemas['base_freebase']+'/type/object/name',
                                      soundtrack['text']

          @virtuoso_writer.new_triple soundtrack_uri,
                                      schemas['base_freebase']+'type/object/type',
                                      schemas['base_freebase']+'/music/soundtrack',
                                      literal: false

          update_pav soundtrack_uri
        end
      end
    end

    def update_production_companies(topic_description, topic_id)
      companies= topic_description['/film/film/production_companies']
      if companies
        companies['values'].each do |company|
          company_uri = schemas['base_freebase']+company['id']

          @virtuoso_writer.delete_triple subject: schemas['base_freebase']+company['id']

          @virtuoso_writer.new_triple schemas['base_freebase']+topic_id,
                                      schemas['base_freebase']+'/film/film/production_companies',
                                      company_uri,
                                      literal: false

          @virtuoso_writer.new_triple company_uri,
                                      schemas['base_freebase']+'/type/object/name',
                                      company['text']

          @virtuoso_writer.new_triple company_uri,
                                      schemas['base_freebase']+'type/object/type',
                                      schemas['base_freebase']+'/film/production_company',
                                      literal: false

          update_pav company_uri
        end
      end
    end

    def update_distributors(topic_description, topic_id)
      distributors= topic_description['/film/film/distributors']
      if distributors
        distributors['values'].each do |distributor|
          film_film_distributor_relationship = distributor['property']['/film/film_film_distributor_relationship/distributor']
          if film_film_distributor_relationship
          company = film_film_distributor_relationship['values'].first

          distributor_uri = schemas['base_freebase']+company['id']

            @virtuoso_writer.delete_triple subject: distributor_uri

            @virtuoso_writer.new_triple schemas['base_freebase']+topic_id,
                                        schemas['base_freebase']+'/film/film/distributors',
                                        distributor_uri,
                                        literal: false

            @virtuoso_writer.new_triple distributor_uri,
                                        schemas['base_freebase']+'/type/object/name',
                                        company['text']

            @virtuoso_writer.new_triple distributor_uri,
                                        schemas['base_freebase']+'type/object/type',
                                        schemas['base_freebase']+'/film/film_distributor',
                                        literal: false

            update_pav distributor_uri
          end
        end
      end
    end

    def update_cast(topic_description, topic_id)
      performances = topic_description['/film/film/starring']
      if performances
        performances['values'].each do |performance|
          # a performance is an object with reference to a character and an actor
          starring_uri = schemas['base_freebase'] + '/film/film/starring'
          performance_uri = schemas['base_freebase'] + performance['id']
          movie_uri = schemas['base_freebase'] + topic_id

          @virtuoso_writer.delete_triple subject: performance_uri

          @virtuoso_writer.new_triple movie_uri,
                                      starring_uri,
                                      performance_uri,
                                      literal: false

          @virtuoso_writer.new_triple performance_uri,
                                      schemas['base_freebase']+'/type/object/type',
                                      schemas['base_freebase']+'/film/performance',
                                      literal: false

          update_pav performance_uri

          if performance['property'] and performance['property']['/film/performance/actor']
            performance['property']['/film/performance/actor']['values'].each do |actor|
              actor_uri = schemas['base_freebase']+actor['id']

              @virtuoso_writer.delete_triple subject: actor_uri

              @virtuoso_writer.new_triple performance_uri,
                                          schemas['base_freebase']+'/film/performance/actor',
                                          actor_uri,
                                          literal: false

              @virtuoso_writer.new_triple actor_uri,
                                          schemas['base_freebase']+'/type/object/name',
                                          actor['text']


              p "intermediate query..." if @verbose
              retrieve_topic actor['id'] do |response|
                # types
                if response['/type/object/type']
                  response['/type/object/type']['values'].each do |type|
                    @virtuoso_writer.new_triple actor_uri,
                                                schemas['base_freebase']+'/type/object/type',
                                                schemas['base_freebase'] + type['id'],
                                                literal: false
                  end
                end

                # birthdate
                if response['/people/person/date_of_birth']
                  response['/people/person/date_of_birth']['values'].each do |birthdate|
                    @virtuoso_writer.new_triple actor_uri,
                                                schemas['base_freebase']+'/people/person/date_of_birth',
                                                birthdate['value']
                  end
                end

                # birthplace
                if response['/people/person/place_of_birth']
                  response['/people/person/place_of_birth']['values'].each do |birthplace|
                    @virtuoso_writer.new_triple actor_uri,
                                                schemas['base_freebase']+'/people/person/place_of_birth',
                                                schemas['base_freebase']+birthplace['id']
                  end
                end

                # alias
                if response['/common/topic/alias']
                  response['/common/topic/alias']['values'].each do |alternative_name|
                    @virtuoso_writer.new_triple actor_uri,
                                                schemas['base_freebase']+'/common/topic/alias',
                                                schemas['base_freebase']+alternative_name['value']
                  end
                end
              end

              update_pav actor_uri
            end
          end

          if performance['property'] and performance['property']['/film/performance/character']
            performance['property']['/film/performance/character']['values'].each do |character|
              character_uri = schemas['base_freebase']+character['id']

              @virtuoso_writer.delete_triple subject: character_uri

              @virtuoso_writer.new_triple performance_uri,
                                          schemas['base_freebase']+'/film/performance/character',
                                          character_uri,
                                          literal: false

              @virtuoso_writer.new_triple character_uri,
                                          schemas['base_freebase']+'/type/object/name',
                                          character['text']

              @virtuoso_writer.new_triple character_uri,
                                          schemas['base_freebase']+'/type/object/type',
                                          schemas['base_freebase'] + '/film/performance/character',
                                          literal: false

              update_pav character_uri
            end
          end
        end
      end
    end

    def update_crew(topic_description, topic_id)
      film_crew_gigs = topic_description['/film/film/other_crew']
      if film_crew_gigs
        film_crew_gigs['values'].each do |film_crew_gig|
          film_crew_gig_uri = schemas['base_freebase']+film_crew_gig['id']
          movie_uri = schemas['base_freebase']+topic_id
          @virtuoso_writer.delete_triple subject: film_crew_gig_uri

          @virtuoso_writer.new_triple movie_uri,
                                      schemas['base_freebase']+'/film/film/other_crew',
                                      film_crew_gig_uri,
                                      literal: false

          @virtuoso_writer.new_triple film_crew_gig_uri,
                                      schemas['base_freebase']+'/type/object/type',
                                      schemas['base_freebase']+'/film/performance',
                                      literal: false

          update_pav film_crew_gig_uri

          if film_crew_gig['property']['/film/film_crew_gig/crewmember']
            film_crew_gig['property']['/film/film_crew_gig/crewmember']['values'].each do |member|
              member_uri = schemas['base_freebase']+member['id']
              @virtuoso_writer.delete_triple subject: member_uri

              @virtuoso_writer.new_triple film_crew_gig_uri,
                                          schemas['base_freebase']+'/film/film_crew_gig/crewmember',
                                          member_uri,
                                          literal: false

              @virtuoso_writer.new_triple member_uri,
                                          schemas['base_freebase']+'/type/object/name',
                                          schemas['base_freebase']+member['text']

              @virtuoso_writer.new_triple member_uri,
                                          schemas['base_freebase']+'/type/object/type',
                                          schemas['base_freebase'] + '/film/film_crewmember',
                                          literal: false

              p "intermediate query..." if @verbose
              retrieve_topic member['id'] do |response|
                # birthdate
                if response['/people/person/date_of_birth']
                  response['/people/person/date_of_birth']['values'].each do |birthdate|
                    @virtuoso_writer.new_triple member_uri,
                                                schemas['base_freebase']+'/people/person/date_of_birth',
                                                birthdate['value']
                  end
                end

                # birthplace
                if response['/people/person/place_of_birth']
                  response['/people/person/place_of_birth']['values'].each do |birthplace|
                    @virtuoso_writer.new_triple member_uri,
                                                schemas['base_freebase']+'/people/person/place_of_birth',
                                                schemas['base_freebase']+birthplace['id']
                  end
                end

                # alias
                if response['/common/topic/alias']
                  response['/common/topic/alias']['values'].each do |alternative_name|
                    @virtuoso_writer.new_triple member_uri,
                                                schemas['base_freebase']+'/common/topic/alias',
                                                schemas['base_freebase']+alternative_name['value']
                  end
                end
              end

              update_pav member_uri
            end
          end

          if film_crew_gig['property']['/film/film_crew_gig/film_crew_role']
            film_crew_gig['property']['/film/film_crew_gig/film_crew_role']['values'].each do |role|
              role_uri = schemas['base_freebase']+role['id']
              @virtuoso_writer.delete_triple subject: role_uri

              @virtuoso_writer.new_triple film_crew_gig_uri,
                                          schemas['base_freebase']+'/film/film_crew_gig/film_crew_role',
                                          role_uri,
                                          literal: false

              @virtuoso_writer.new_triple role_uri,
                                          schemas['base_freebase']+'/type/object/name',
                                          schemas['base_freebase']+role['text']

              @virtuoso_writer.new_triple role_uri,
                                          schemas['base_freebase']+'/type/object/type',
                                          schemas['base_freebase'] + '/film/film_crewmember',
                                          literal: false
              update_pav role_uri
            end
          end
        end
      end
    end

    def write_person(person, locator, topic_id)

      person_uri = schemas['base_freebase'] + person['id']
      @virtuoso_writer.delete_triple subject: person_uri

      @virtuoso_writer.new_triple schemas['base_freebase']+topic_id,
                                  schemas['base_freebase']+locator,
                                  person_uri,
                                  literal: false
      @virtuoso_writer.new_triple person_uri,
                                  schemas['base_freebase'] + '/type/object/name',
                                  person['text']



      p "intermediate query..." if @verbose
      retrieve_topic person['id'] do |response|
        # types
        if response['/type/object/type']
          response['/type/object/type']['values'].each do |type|
            @virtuoso_writer.new_triple person_uri,
                                        schemas['base_freebase']+'/type/object/type',
                                        schemas['base_freebase'] + type['id'],
                                        literal: false
          end
        end

        # birthdate
        if response['/people/person/date_of_birth']
          response['/people/person/date_of_birth']['values'].each do |birthdate|
            @virtuoso_writer.new_triple person_uri,
                                        schemas['base_freebase']+'/people/person/date_of_birth',
                                        birthdate['value']
          end
        end

        # birthplace
        if response['/people/person/place_of_birth']
          response['/people/person/place_of_birth']['values'].each do |birthplace|
            @virtuoso_writer.new_triple person_uri,
                                        schemas['base_freebase']+'/people/person/place_of_birth',
                                        schemas['base_freebase']+birthplace['id']
          end
        end

        # alias
        if response['/common/topic/alias']
          response['/common/topic/alias']['values'].each do |alternative_name|
            @virtuoso_writer.new_triple person_uri,
                                        schemas['base_freebase']+'/common/topic/alias',
                                        schemas['base_freebase']+alternative_name['value']
          end
        end
      end


    end

    def update_persons(topic_description, topic_id)
      %w( /film/film/cinematography
        /film/film/costume_design_by
        /film/film/edited_by
        /film/film/executive_produced_by
        /film/film/film_art_direction_by
        /film/film/film_casting_director
        /film/film/film_production_design_by
        /film/film/music
        /film/film/produced_by
        /film/film/story_by
        /film/film/written_by
        /film/film/directed_by).each do |locator|

        topic = topic_description[locator]
        if topic
          topic['values'].each do |person|
            write_person person, locator, topic_id
          end
        end
      end
    end


    def retrieve_text(topic_description, topic_id, locator)
      retrieve topic_description, topic_id, locator, 'text'
    end

    def retrieve_value(topic_description, topic_id, locator)
      retrieve topic_description, topic_id, locator, 'value'
    end

    def retrieve_uri(topic_description, topic_id, locator)
      return unless topic_description[locator]
      topic_description[locator]['values'].each do |element|
        @virtuoso_writer.new_triple schemas['base_freebase']+topic_id,
                                    schemas['base_freebase']+locator,
                                    schemas['base_freebase']+element['id'],
                                    literal: false
      end
    end

    def retrieve_date(topic_description, topic_id, locator)
      return unless topic_description[locator]
      topic_description[locator]['values'].each do |element|
        @virtuoso_writer.new_triple schemas['base_freebase']+topic_id,
                                    schemas['base_freebase']+locator,
                                    RDF::Literal.new(element['value'], datatype: RDF::XSD.date)
      end
    end

    def retrieve(topic_description, topic_id, locator, element_locator)
      return unless topic_description[locator]
      topic_description[locator]['values'].each do |element|
        @virtuoso_writer.new_triple schemas['base_freebase']+topic_id,
                                    schemas['base_freebase']+locator,
                                    element[element_locator]
      end
    end

    def retrieve_element(topic_response, locator)
      return unless topic_response[locator]
      topic_response[locator]['values'].each do |element|
        yield element['text'] if block_given?
      end
    end

    def retrieve_topic(topic_id)
      @crawler.read_topic topic_id do |topic|
        yield topic if block_given?
      end
    end

    def update_pav(subject)
      @virtuoso_writer.new_triple subject,
                                  schemas['pav_lastupdateon'],
                                  RDF::Literal.new(Date.today)
    end

    private
    def schemas
      @schemas ||= load_schemas
    end

    def load_schemas
      file = YAML.load_file '../config/namespaces.yml'
      file['schemas']
    end
  end
end
