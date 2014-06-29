require 'yaml'
require 'json'

class FreebaseUpdater::Updater
  NS = 'http://rdf.freebase.com/ns'
  XSD = 'http://www.w3.org/2001/XMLSchema#'

  def initialize
    @crawler = FreebaseCrawler::Crawler.new
    @virtuoso = FreebaseUpdater::VirtuosoWriter.new
    @receiver = FreebaseUpdater::MsgConsumer.new

    puts "listening on queue #{@receiver.queue_name :movie_id}"
    @receiver.subscribe(type: :movie_id) { |movie_id| update(movie_id) }
  end

  def update(topic_id)
    # long queries are not necessarily answered, thus we split it
    p "Looking up MID #{topic_id} ..."
    retrieve_topic topic_id do |topic_description|
      # try to delete existing triples for movie first
      @virtuoso.delete_triple subject: NS + topic_id

      update_primitives topic_description, topic_id
      update_persons topic_description, topic_id
      p 'done'
    end
  end

  def update_primitives(topic_description, topic_id)
    # strings
    %W( /common/topic/description
        /common/topic/official_website
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

    # dates
    retrieve_date topic_description, topic_id, '/film/film/initial_release_date'

    # other objects
    retrieve_uri topic_description, topic_id, '/type/object/type'
    # these movies should be fetched automatically
    retrieve_uri topic_description, topic_id, '/film/film/prequel'
    retrieve_uri topic_description, topic_id, '/film/film/sequel'


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

  def write_person(person, locator, topic_id)
    person_uri = NS + person['id']
    @virtuoso.delete_triple subject: person_uri

    @virtuoso.new_triple NS+topic_id, NS+locator, person_uri, literal: false
    @virtuoso.new_triple person_uri, NS + '/type/object/name', person['text']
    # TODO collect more, eg type
  end



  def retrieve_text(topic_description, topic_id, locator)
    retrieve topic_description, topic_id, locator, 'text'
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

end

#https://www.googleapis.com/freebase/v1/topic/m/07f_t4



#retrieve_text topic_description, topic_id, '/film/film/country' #TODO Country
##retrieve_text topic_description, topic_id, '/film/film/film_festivals' #TODO Event?
##retrieve_text topic_description, topic_id, '/film/film/film_series' #TODO Series
#retrieve_text topic_description, topic_id, '/film/film/genre' #TODO Genre
#retrieve_text topic_description, topic_id, '/film/film/production_companies' #TODO Company
#retrieve_text topic_description, topic_id, '/film/film/production_companies' #TODO Company
##retrieve_text topic_description, topic_id, '/film/film/release_date_s' #TODO Company # additional releases (festivals etc), do we need this?
#retrieve_text topic_description, topic_id, '/film/film/soundtrack' #TODO Music
#retrieve_text topic_description, topic_id, '/film/film/language' #TODO Language?
#
#
## go deeper!!!
##retrieve_text topic_description, topic_id, '/film/film/distributors' #TODO Company?  GO DEEPER!
##retrieve_text topic_description, topic_id, '/film/film/estimated_budget' #TODO Go deeper
##retrieve_text topic_description, topic_id, '/film/film/other_crew' #TODO go deeper
##retrieve_text topic_description, topic_id, '/film/film/runtime' #TODO go deeper
##retrieve_text topic_description, topic_id, '/film/film/starring' #TODO go deeper, persons, characters...
