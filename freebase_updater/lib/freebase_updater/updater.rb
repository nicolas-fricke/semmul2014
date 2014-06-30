require 'yaml'
require 'json'

class FreebaseUpdater::Updater
  NS = 'http://rdf.freebase.com/ns'
  XSD = 'http://www.w3.org/2001/XMLSchema#'
  PAV_LASTUPDATEON = 'http://purl.org/pav/lastUpdateOn'

  def initialize
    @crawler = FreebaseCrawler::Crawler.new
    @virtuoso = FreebaseUpdater::VirtuosoWriter.new
    @receiver = FreebaseUpdater::MsgConsumer.new
    @publisher = FreebaseUpdater::MsgPublisher.new

    puts "listening on queue #{@receiver.queue_name :movie_id}"
    @receiver.subscribe(type: :movie_id) { |movie_id| update(movie_id) }
  end

  def update(topic_id)
    # long queries are not necessarily answered, thus we split it
    p "Looking up MID #{topic_id} ..."
    start_time = Time.now

    retrieve_topic topic_id do |topic_description|
      movie_uri = NS + topic_id

      # try to delete existing triples for movie first
      @virtuoso.delete_triple subject: movie_uri

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

      p "Finished within #{Time.now - start_time}s"
      @publisher.enqueue_uri :movie_uri, movie_uri
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
      sum = 0
      runtimes['values'].each do |runtime|
        # there are several runtimes
        # initially the idea was to distinguish different cuts (TV, cinema, director's cut...)
        # usually the type is not provided, the runtime in claimed differently though
        # for now, we simply average the result
        # TODO think of a better solution here
        runtime['property']['/film/film_cut/runtime']['values'].each do |cut|
          sum += cut['value']
        end
      end
      sum /= runtimes['values'].count
      @virtuoso.new_triple NS+topic_id, NS+'/film/film_cut/runtime', sum
    end
  end

  def update_language(topic_description, topic_id)
    languages = topic_description['/film/film/language']
    if languages
      languages['values'].each do |language|
        @virtuoso.delete_triple subject: NS+language['id']

        @virtuoso.new_triple NS+topic_id, NS+'/film/film/language', NS+language['id']
        @virtuoso.new_triple NS+language['id'], NS+'/type/object/name', language['text']
        @virtuoso.new_triple NS+language['id'], NS+'/language/human_language/iso_639_1_code', language['lang']
        @virtuoso.new_triple NS+language['id'], NS+'type/object/type', NS+'/language/human_language'
        update_pav NS+language['id']
      end
    end
  end

  def update_country(topic_description, topic_id)
    countries = topic_description['/film/film/country']
    if countries
      countries['values'].each do |country|
        @virtuoso.delete_triple subject: NS+country['id']

        @virtuoso.new_triple NS+topic_id, NS+'/film/film/country', NS+country['id']
        @virtuoso.new_triple NS+country['id'], NS+'/type/object/name', country['text']
        @virtuoso.new_triple NS+country['id'], NS+'type/object/type', NS+'/location/country'
        update_pav NS+country['id']
      end
    end
  end

  def update_genres(topic_description, topic_id)
    genres= topic_description['/film/film/genre']
    if genres['values']
      genres['values'].each do |genre|
        @virtuoso.delete_triple subject: NS+genre['id']

        @virtuoso.new_triple NS+topic_id, NS+'/film/film/genre', NS+genre['id']
        @virtuoso.new_triple NS+genre['id'], NS+'/type/object/name', genre['text']
        @virtuoso.new_triple NS+genre['id'], NS+'type/object/type', NS+'/film/film_genre'
        update_pav NS+genre['id']
      end
    end
  end

  def update_soundtrack(topic_description, topic_id)
    soundtracks= topic_description['/film/film/soundtrack']
    if soundtracks
      soundtracks['values'].each do |soundtrack|
        @virtuoso.delete_triple subject: NS+soundtrack['id']

        @virtuoso.new_triple NS+topic_id, NS+'/film/film/soundtrack', NS+soundtrack['id']
        @virtuoso.new_triple NS+soundtrack['id'], NS+'/type/object/name', soundtrack['text']
        @virtuoso.new_triple NS+soundtrack['id'], NS+'type/object/type', NS+'/music/soundtrack'
        update_pav NS+soundtrack['id']
      end
    end
  end

  def update_production_companies(topic_description, topic_id)
    companies= topic_description['/film/film/production_companies']
    if companies
      companies['values'].each do |company|
        @virtuoso.delete_triple subject: NS+company['id']

        @virtuoso.new_triple NS+topic_id, NS+'/film/film/production_companies', NS+company['id']
        @virtuoso.new_triple NS+company['id'], NS+'/type/object/name', company['text']
        @virtuoso.new_triple NS+company['id'], NS+'type/object/type', NS+'/film/production_company'
        update_pav NS+company['id']
      end
    end
  end

  def update_distributors(topic_description, topic_id)
    distributors= topic_description['/film/film/distributors']
    if distributors
      distributors['values'].each do |distributor|
        company = distributor['property']['/film/film_film_distributor_relationship/distributor']['values'].first

        @virtuoso.delete_triple subject: NS+company['id']

        @virtuoso.new_triple NS+topic_id, NS+'/film/film/distributors', NS+company['id']
        @virtuoso.new_triple NS+company['id'], NS+'/type/object/name', company['text']
        @virtuoso.new_triple NS+company['id'], NS+'type/object/type', NS+'/film/film_distributor'
        update_pav NS+company['id']
      end
    end
  end

  def update_cast(topic_description, topic_id)
    performances = topic_description['/film/film/starring']
    if performances
      performances['values'].each do |performance|
        # a performance is an object with reference to a character and an actor
        starring_uri = NS + '/film/film/starring'
        performance_uri = NS + performance['id']
        movie_uri = NS + topic_id

        @virtuoso.delete_triple subject: performance_uri
        @virtuoso.new_triple movie_uri, starring_uri, performance_uri, literal: false
        @virtuoso.new_triple performance_uri, NS+'/type/object/type', NS+'/film/performance', literal: false
        update_pav performance_uri

        if performance['property']['/film/performance/actor']
          performance['property']['/film/performance/actor']['values'].each do |actor|
            @virtuoso.delete_triple subject: NS+actor['id']

            @virtuoso.new_triple performance_uri, NS+'/film/performance/actor', NS+actor['id'] , literal: false
            @virtuoso.new_triple NS+actor['id'], NS+'/type/object/name', actor['text']
            update_pav NS+actor['id']

            # query their type
            query = {
                'mid' => actor['id'],
                'type' => []
            }
            @crawler.read_mql query do |response|
              response['type'].each do |type|
                @virtuoso.new_triple NS+actor['id'], NS+'/type/object/type', NS + type
              end
            end

          end
        end

        if performance['property']['/film/performance/character']
          performance['property']['/film/performance/character']['values'].each do |character|
            @virtuoso.delete_triple subject: NS+character['id']

            @virtuoso.new_triple performance_uri, NS+'/film/performance/character', NS+character['id'] , literal: false
            @virtuoso.new_triple NS+character['id'], NS+'/type/object/name', character['text']
            @virtuoso.new_triple NS+character['id'], NS+'/type/object/type', NS + '/film/performance/character'
            update_pav NS+character['id']
          end
        end
      end
    end
  end

  def update_crew(topic_description, topic_id)
    film_crew_gigs = topic_description['/film/film/other_crew']
    if film_crew_gigs
      film_crew_gigs['values'].each do |film_crew_gig|
        film_crew_gig_uri = NS+film_crew_gig['id']
        movie_uri = NS+topic_id
        @virtuoso.delete_triple subject: film_crew_gig_uri
        @virtuoso.new_triple movie_uri, NS+'/film/film/other_crew', film_crew_gig_uri, literal: false
        @virtuoso.new_triple film_crew_gig_uri, NS+'/type/object/type', NS+'/film/performance', literal: false
        update_pav film_crew_gig_uri

        if film_crew_gig['property']['/film/film_crew_gig/crewmember']
          film_crew_gig['property']['/film/film_crew_gig/crewmember']['values'].each do |member|
            member_uri = NS+member['id']
            @virtuoso.delete_triple subject: member_uri

            @virtuoso.new_triple film_crew_gig_uri, NS+'/film/film_crew_gig/crewmember', member_uri, literal: false
            @virtuoso.new_triple member_uri, NS+'/type/object/name', NS+member['text']
            @virtuoso.new_triple member_uri, NS+'/type/object/type', NS + '/film/film_crewmembe'
            update_pav member_uri
          end
        end

        if film_crew_gig['property']['/film/film_crew_gig/film_crew_role']
          film_crew_gig['property']['/film/film_crew_gig/film_crew_role']['values'].each do |role|
            role_uri = NS+role['id']
            @virtuoso.delete_triple subject: role_uri

            @virtuoso.new_triple film_crew_gig_uri, NS+'/film/film_crew_gig/film_crew_role', role_uri, literal: false
            @virtuoso.new_triple role_uri, NS+'/type/object/name', NS+role['text']
            @virtuoso.new_triple role_uri, NS+'/type/object/type', NS + '/film/film_crewmembe'
            update_pav role_uri
          end
        end
      end
    end
  end

  def write_person(person, locator, topic_id)

    person_uri = NS + person['id']
    @virtuoso.delete_triple subject: person_uri

    @virtuoso.new_triple NS+topic_id, NS+locator, person_uri, literal: false
    @virtuoso.new_triple person_uri, NS + '/type/object/name', person['text']

    # TODO collect more, eg type
    # query their type
    query = {
        'mid' => person['id'],
        'type' => []
    }
    @crawler.read_mql query do |response|
      response['type'].each do |type|
        @virtuoso.new_triple person_uri, NS + '/type/object/type', NS + type
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
        /film/film/written_by).each do |locator|

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
      @virtuoso.new_triple NS+topic_id, NS+locator, NS+element['id'], literal: false
    end
    end

  def retrieve_date(topic_description, topic_id, locator)
    return unless topic_description[locator]
    topic_description[locator]['values'].each do |element|
      @virtuoso.new_triple NS+topic_id, NS+locator, set_xsd_type(element['value'], 'datetime')
    end
  end

  def retrieve(topic_description, topic_id, locator, element_locator)
    return unless topic_description[locator]
    topic_description[locator]['values'].each do |element|
      @virtuoso.new_triple NS+topic_id, NS+locator, element[element_locator]
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

  def set_xsd_type(literal, type)
    "#{literal}^^#{XSD}#{type}"
  end

  def update_pav(subject)
    @virtuoso.new_triple  subject,
                          PAV_LASTUPDATEON,
                          (set_xsd_type DateTime.now, 'dateTime')
  end

end

# not stored yet
#'/film/film/estimated_budget' #TODO nested
#'/film/film/other_crew' #TODO nested
