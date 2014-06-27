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
      write_triples_for topic_id, topic_description
    end

  end

  def write_triples_for(topic_id, topic_description)
    p "writing triples"


    # literals
    retrieve_text topic_description, topic_id, '/common/topic/description'
    retrieve_text topic_description, topic_id, '/common/topic/official_website'
    retrieve_text topic_description, topic_id, '/film/film/film_format'
    retrieve_text topic_description, topic_id, '/film/film/metacritic_id'
    retrieve_text topic_description, topic_id, '/film/film/netflix_id'
    retrieve_text topic_description, topic_id, '/film/film/rottentomatoes_id'
    retrieve_text topic_description, topic_id, '/film/film/subjects'
    retrieve_text topic_description, topic_id, '/film/film/traileraddict_id'
    retrieve_text topic_description, topic_id, '/film/film/trailers'
    retrieve_text topic_description, topic_id, '/media_common/netflix_title/netflix_genres'

    # persons # TODO query information on them separately
    retrieve_uri topic_description, topic_id, '/film/film/cinematography'
    retrieve_uri topic_description, topic_id, '/film/film/costume_design_by'
    retrieve_uri topic_description, topic_id, '/film/film/edited_by'
    retrieve_uri topic_description, topic_id, '/film/film/executive_produced_by'
    retrieve_uri topic_description, topic_id, '/film/film/film_art_direction_by'
    retrieve_uri topic_description, topic_id, '/film/film/film_casting_director'
    retrieve_uri topic_description, topic_id, '/film/film/film_production_design_by'
    retrieve_uri topic_description, topic_id, '/film/film/music'
    retrieve_uri topic_description, topic_id, '/film/film/produced_by'
    retrieve_uri topic_description, topic_id, '/film/film/story_by'
    retrieve_uri topic_description, topic_id, '/film/film/written_by'

    # dates
    retrieve_date topic_description, topic_id, '/film/film/initial_release_date'

    # other objects
    retrieve_uri topic_description, topic_id, '/film/film/prequel' #TODO Movie
    retrieve_uri topic_description, topic_id, '/film/film/prequel' #TODO Movie

    retrieve_text topic_description, topic_id, '/film/film/country' #TODO Country
    #retrieve_text topic_description, topic_id, '/film/film/film_festivals' #TODO Event?
    #retrieve_text topic_description, topic_id, '/film/film/film_series' #TODO Series
    retrieve_text topic_description, topic_id, '/film/film/genre' #TODO Genre
    retrieve_text topic_description, topic_id, '/film/film/production_companies' #TODO Company
    retrieve_text topic_description, topic_id, '/film/film/production_companies' #TODO Company
    #retrieve_text topic_description, topic_id, '/film/film/release_date_s' #TODO Company # additional releases (festivals etc), do we need this?
    retrieve_text topic_description, topic_id, '/film/film/sequel' #TODO Movie
    retrieve_text topic_description, topic_id, '/film/film/soundtrack' #TODO Music
    retrieve_text topic_description, topic_id, '/film/film/language' #TODO Language?


    # go deeper!!!
    #retrieve_text topic_description, topic_id, '/film/film/distributors' #TODO Company?  GO DEEPER!
    #retrieve_text topic_description, topic_id, '/film/film/estimated_budget' #TODO Go deeper
    #retrieve_text topic_description, topic_id, '/film/film/other_crew' #TODO go deeper
    #retrieve_text topic_description, topic_id, '/film/film/runtime' #TODO go deeper
    #retrieve_text topic_description, topic_id, '/film/film/starring' #TODO go deeper, persons, characters...

    p 'done'
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
      @virtuoso.new_triple NS+topic_id, NS+locator, set_xsd_type(NS+element['value'], 'datetime')
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
