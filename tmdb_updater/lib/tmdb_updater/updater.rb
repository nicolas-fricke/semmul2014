require 'yaml'
require 'themoviedb'

class TMDbUpdater::Updater

  def initialize
    load_schemas
    @publisher = MsgPublisher.new
    @publisher.set_queue 'raw_tmdb'
    @virtuoso_writer = VirtuosoWriter.new
    @virtuoso_writer.set_graph 'raw'
    initialize_tmdb_api
  end

  def register_receiver
    @receiver = MsgConsumer.new
    @receiver.set_queue 'source_tmdb'
    @receiver.subscribe(type: :movie_id) { |movie_id| update(movie_id) }
  end

  # TODO what if triple already in database? overwrite?
  def update(movie_id)
    # all data about movie with movie_id
    movie_details = get_movie_for_id movie_id
    uri_movie = "#{@schemas['base_tmdb']}movie/#{movie_id}"
    update_movie movie_details, uri_movie

    # alternative titles
    puts 'alternative titles'
    movie_titles = get_titles_for_id movie_id
    update_titles movie_titles, uri_movie

    # cast
    puts 'cast'
    movie_cast = get_casts_for_id movie_id
    update_cast movie_cast, uri_movie

    # crew
    puts 'crew'
    movie_crew = get_crew_for_id movie_id
    update_crew movie_crew, uri_movie

    # collection
    puts 'collection'
    update_collection movie_details, uri_movie

    # genres
    puts 'genres'
    update_genres movie_details, uri_movie

    # production companies
    puts 'companies'
    update_companies movie_details, uri_movie

    # production countries
    puts 'countries'
    update_countries movie_details, uri_movie

    # releases
    puts 'releases'
    movie_releases = get_releases_for_id movie_id
    update_releases movie_releases, uri_movie

    # spoken languages
    puts 'languages'
    movie_languages = movie_details.spoken_languages
    update_languages movie_languages, uri_movie

    # translations
    puts 'translations'
    movie_translations = get_translations_for_id movie_id
    update_translations movie_translations, uri_movie

    # enqueue
    puts 'ENQUEUE :-D'
    @publisher.enqueue :movie_uri, uri_movie
  end
  
  def update_movie(movie, uri_movie)
    # try to delete existing triples for movie first
    @virtuoso_writer.delete_triple(
        subject: uri_movie)

    # add new triples
    @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['rdf']}type", "#{@schemas['tmdb']}Movie", literal: false
    )
    if movie.id.to_s.length > 0
      @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['tmdb']}movie/id", movie.id
      )
    end
    if movie.adult.to_s.length > 0
      @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['tmdb']}movie/adult", movie.adult
      )
    end
    if movie.backdrop_path.to_s.length > 0
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/backdrop_path", movie.backdrop_path
      )
    end
    if movie.budget.to_s.length > 0
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/budget", movie.budget
      )
    end
    if movie.homepage.to_s.length > 0
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/homepage", movie.homepage
      )
    end
    if movie.imdb_id.to_s.length > 0
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/imdb_id", movie.imdb_id
      )
    end
    if movie.original_title.to_s.length > 0
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/original_title", movie.original_title
      )
    end
    @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['tmdb']}movie/overview", movie.overview
    )
    @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['tmdb']}movie/popularity", movie.popularity
    )
    @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['tmdb']}movie/poster_path", movie.poster_path
    )
    @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['tmdb']}movie/release_date", movie.release_date
    )
    @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['tmdb']}movie/revenue", movie.revenue
    )
    @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['tmdb']}movie/runtime", movie.runtime
    )
    @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['tmdb']}movie/status", movie.status
    )
    @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['tmdb']}movie/tagline", movie.tagline
    )
    @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['tmdb']}movie/title", movie.title
    )
    @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['tmdb']}movie/vote_average", movie.vote_average
    )
    @virtuoso_writer.new_triple(
        uri_movie, "#{@schemas['tmdb']}movie/vote_count", movie.vote_count
    )
    @virtuoso_writer.new_triple(
        uri_movie, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
    )
  end
  
  def update_titles(titles, uri_movie)
    movie_titles = titles['titles']   # id is ignored because its identical with movie_id
    movie_titles.each do |title|
      uri_title = "#{uri_movie}/alternative_titles/#{title['iso_3166_1']}"
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/alternative_titles/titles", uri_title, literal: false
      )

      # try to delete existing triples for title first
      @virtuoso_writer.delete_triple(
          subject: uri_title)

      # add new triples
      @virtuoso_writer.new_triple(
          uri_title, "#{@schemas['rdf']}type", "#{@schemas['tmdb']}Alternative_Title", literal: false
      )
      @virtuoso_writer.new_triple(
          uri_title, "#{@schemas['tmdb']}movie/alternative_titles/titles/iso_3166_1", title['iso_3166_1']
      )
      @virtuoso_writer.new_triple(
          uri_title, "#{@schemas['tmdb']}movie/alternative_titles/titles/title", title['title']
      )
      @virtuoso_writer.new_triple(
          uri_title, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      )
    end
  end

  def update_cast(movie_casts, uri_movie)
    movie_casts.each do |cast|
      uri_cast = "#{uri_movie}/cast/#{cast['id']}"
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/credits/cast", uri_cast, literal: false
      )

      # try to delete existing triples for cast first
      @virtuoso_writer.delete_triple(
          subject: uri_cast)

      # add new triples
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['rdf']}type", "#{@schemas['tmdb']}Cast", literal: false
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}cast/id", cast['id']
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}cast/cast_id", cast['cast_id']
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}cast/character", cast['character']
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}cast/name", cast['name']
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}cast/order", cast['order']
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}cast/profile_path", cast['profile_path']
      )
      uri_person = "#{@schemas['base_tmdb']}person/#{cast['id']}"
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}cast/person", uri_person
      )
      @virtuoso_writer.new_triple(
          uri_cast, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      )
      # update person
      update_person cast['id'], uri_person
    end
  end
  
  def update_crew(movie_crew, uri_movie)
    movie_crew.each do |crew|
      uri_crew = "#{uri_movie}/crew/#{crew['id']}"
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/credits/crew", uri_crew, literal: false
      )

      # try to delete existing triples for crew first
      @virtuoso_writer.delete_triple(
          subject: uri_crew)

      # add new triples
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['rdf']}type", "#{@schemas['tmdb']}Crew", literal: false
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}crew/id", crew['id']
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}crew/department", crew['department']
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}crew/job", crew['job']
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}crew/name", crew['name']
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}crew/profile_path", crew['profile_path']
      )
      uri_person = "#{@schemas['base_tmdb']}person/#{crew['id']}"
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}crew/person", uri_person
      )
      @virtuoso_writer.new_triple(
          uri_crew, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      )
      # update person
      update_person crew['id'], uri_person
    end
  end
  
  def update_collection(movie, uri_movie)
    collection_id = movie.belongs_to_collection['id'] if movie.belongs_to_collection
    if collection_id
      collection = get_collection_for_id collection_id
      uri_collection = "#{@schemas['base_tmdb']}collection/#{collection_id}"
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/belongs_to_collection", uri_collection, literal: false
      )

      # try to delete existing triples for collection first
      @virtuoso_writer.delete_triple(
          subject: uri_collection)

      # add new triples
      @virtuoso_writer.new_triple(
          uri_collection, "#{@schemas['rdf']}type", "#{@schemas['tmdb']}Collection", literal: false
      )
      @virtuoso_writer.new_triple(
          uri_collection, "#{@schemas['tmdb']}movie/belongs_to_collection/id", collection_id
      )
      @virtuoso_writer.new_triple(
          uri_collection, "#{@schemas['tmdb']}movie/belongs_to_collection/name", collection.name
      )
      @virtuoso_writer.new_triple(
          uri_collection, "#{@schemas['tmdb']}movie/belongs_to_collection/poster_path", collection.poster_path
      )
      @virtuoso_writer.new_triple(
          uri_collection, "#{@schemas['tmdb']}movie/belongs_to_collection/backdrop_path", collection.backdrop_path
      )
      @virtuoso_writer.new_triple(
          uri_collection, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      )
    end
  end
  
  def update_genres(movie, uri_movie)
    movie_genres = movie.genres
    movie_genres.each do |genre|
      uri_genre = "#{@schemas['base_tmdb']}genre/#{genre['id']}"
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/genres", uri_genre, literal: false
      )

      # try to delete existing triples for genre first
      @virtuoso_writer.delete_triple(
          subject: uri_genre)

      # add new triples
      @virtuoso_writer.new_triple(
          uri_genre, "#{@schemas['rdf']}type", "#{@schemas['tmdb']}Genre", literal: false
      )
      @virtuoso_writer.new_triple(
          uri_genre, "#{@schemas['tmdb']}genres/id", genre['id']
      )
      @virtuoso_writer.new_triple(
          uri_genre, "#{@schemas['tmdb']}genres/name", genre['name']
      )
      @virtuoso_writer.new_triple(
          uri_genre, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      )
    end
  end
  
  def update_companies(movie, uri_movie)
    movie_companies = movie.production_companies
    movie_companies.each do |company|
      uri_company = "#{@schemas['base_tmdb']}company/#{company['id']}"
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/production_companies", uri_company, literal: false
      )

      # try to delete existing triples for company first
      @virtuoso_writer.delete_triple(
          subject: uri_company)

      # add new triples
      @virtuoso_writer.new_triple(
          uri_company, "#{@schemas['rdf']}type", "#{@schemas['tmdb']}Production_Company", literal: false
      )
      @virtuoso_writer.new_triple(
          uri_company, "#{@schemas['tmdb']}movie/production_companies/id", company['id']
      )
      @virtuoso_writer.new_triple(
        uri_company, "#{@schemas['tmdb']}movie/production_companies/name", company['name']
      )
      @virtuoso_writer.new_triple(
          uri_company, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      )
    end
  end
  
  def update_countries(movie, uri_movie)
    movie_countries = movie.production_countries
    movie_countries.each do |country|
      uri_country = "#{@schemas['base_tmdb']}country/#{country['iso_639_1']}"
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/production_countries", uri_country, literal: false
      )

      # try to delete existing triples for country first
      @virtuoso_writer.delete_triple(
          subject: uri_country)

      # add new triples
      @virtuoso_writer.new_triple(
          uri_country, "#{@schemas['rdf']}type", "#{@schemas['tmdb']}Production_Country", literal: false
      )
      @virtuoso_writer.new_triple(
          uri_country, "#{@schemas['tmdb']}movie/production_countries/iso_639_1", country['iso_639_1']
      )
      @virtuoso_writer.new_triple(
          uri_country, "#{@schemas['tmdb']}movie/production_countries/name", country['name']
      )
      @virtuoso_writer.new_triple(
          uri_country, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      )
    end
  end
  
  def update_releases(releases, uri_movie)
    movie_releases = releases['countries']
    movie_releases.each do |release|
      uri_release = "#{uri_movie}/releases/#{release['iso_3166_1']}"
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/releases/countries", uri_release, literal: false
      )

      # try to delete existing triples for release first
      @virtuoso_writer.delete_triple(
          subject: uri_release)

      # add new triples
      @virtuoso_writer.new_triple(
          uri_release, "#{@schemas['rdf']}type", "#{@schemas['tmdb']}Release_Country", literal: false
      )
      @virtuoso_writer.new_triple(
          uri_release, "#{@schemas['tmdb']}movie/releases/countries/iso_3166_1", release['iso_3166_1']
      )
      @virtuoso_writer.new_triple(
          uri_release, "#{@schemas['tmdb']}movie/releases/countries/certification", release['certification']
      )
      @virtuoso_writer.new_triple(
          uri_release, "#{@schemas['tmdb']}movie/releases/countries/release_date", release['release_date']
      )
      @virtuoso_writer.new_triple(
          uri_release, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      )
    end
  end

  def update_languages(movie_languages, uri_movie)
    movie_languages.each do |language|
      uri_language = "#{@schemas['base_tmdb']}language/#{language['iso_639_1']}"
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/spoken_languages", uri_language, literal: false
      )

      # try to delete existing triples for language first
      @virtuoso_writer.delete_triple(
          subject: uri_language)

      # add new triples
      @virtuoso_writer.new_triple(
          uri_language, "#{@schemas['rdf']}type", "#{@schemas['tmdb']}Spoken_Language", literal: false
      )
      @virtuoso_writer.new_triple(
          uri_language, "#{@schemas['tmdb']}movie/spoken_languages/iso_639_1", language['iso_639_1']
      )
      @virtuoso_writer.new_triple(
          uri_language, "#{@schemas['tmdb']}movie/spoken_languages/name", language['name']
      )
      @virtuoso_writer.new_triple(
          uri_language,@schemas['pav_lastupdateon'],(set_xsd_type DateTime.now, 'dateTime')
      )
    end
  end

  def update_translations(translations, uri_movie)
    movie_translations = translations['translations']
    movie_translations.each do |translation|
      uri_translation = "#{@schemas['base_tmdb']}translation/#{translation['iso_639_1']}"
      @virtuoso_writer.new_triple(
          uri_movie, "#{@schemas['tmdb']}movie/translations/translations", uri_translation, literal: false
      )

      # try to delete existing triples for translation first
      @virtuoso_writer.delete_triple(
          subject: uri_translation)

      # add new triples
      @virtuoso_writer.new_triple(
          uri_translation, "#{@schemas['rdf']}type", "#{@schemas['tmdb']}Translation", literal: false
      )
      @virtuoso_writer.new_triple(
          uri_translation, "#{@schemas['tmdb']}movie/translations/translations/iso_639_1", translation['iso_639_1']
      )
      @virtuoso_writer.new_triple(
          uri_translation, "#{@schemas['tmdb']}movie/translations/translations/name", translation['name']
      )
      @virtuoso_writer.new_triple(
          uri_translation, "#{@schemas['tmdb']}movie/translations/translations/english_name", translation['english_name']
      )
      @virtuoso_writer.new_triple(
          uri_translation, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      )
    end
  end

  def get_movie_for_id(movie_id, attempt: 0)
    Tmdb::Movie.detail movie_id
  rescue SocketError => e
    if attempt < 10
      puts "  !!! A socket error occurred, retrying (#{attempt} retries already, id: #{movie_id})"
      sleep 0.5
      get_movie_for_id movie_id, attempt: (attempt + 1)
    else
      puts "  !!! A socket error occurred 10 times, cancel retry for movie with id #{movie_id}"
      raise e
    end
  end

  def get_titles_for_id(movie_id, attempt: 0)
    Tmdb::Movie.alternative_titles movie_id
  rescue SocketError => e
    if attempt < 10
      puts "  !!! A socket error occurred, retrying (#{attempt} retries already, id: #{movie_id})"
      sleep 0.5
      get_titles_for_id movie_id, attempt: (attempt + 1)
    else
      puts "  !!! A socket error occurred 10 times, cancel retry for movie with id #{movie_id}"
      raise e
    end
  end

  def get_casts_for_id(movie_id, attempt: 0)
    Tmdb::Movie.casts movie_id
  rescue SocketError => e
    if attempt < 10
      puts "  !!! A socket error occurred, retrying (#{attempt} retries already, id: #{movie_id})"
      sleep 0.5
      get_casts_for_id movie_id, attempt: (attempt + 1)
    else
      puts "  !!! A socket error occurred 10 times, cancel retry for movie with id #{movie_id}"
      raise e
    end
  end

  def get_crew_for_id(movie_id, attempt: 0)
    Tmdb::Movie.crew movie_id
  rescue SocketError => e
    if attempt < 10
      puts "  !!! A socket error occurred, retrying (#{attempt} retries already, id: #{movie_id})"
      sleep 0.5
      get_crew_for_id movie_id, attempt: (attempt + 1)
    else
      puts "  !!! A socket error occurred 10 times, cancel retry for movie with id #{movie_id}"
      raise e
    end
  end

  def get_collection_for_id(collection_id, attempt: 0)
    Tmdb::Collection.detail collection_id
  rescue SocketError => e
    if attempt < 10
      puts "  !!! A socket error occurred, retrying (#{attempt} retries already, id: #{collection_id})"
      sleep 0.5
      get_collection_for_id collection_id, attempt: (attempt + 1)
    else
      puts "  !!! A socket error occurred 10 times, cancel retry for collection with id #{collection_id}"
      raise e
    end
  end

  def get_releases_for_id(movie_id, attempt: 0)
    Tmdb::Movie.releases movie_id
  rescue SocketError => e
    if attempt < 10
      puts "  !!! A socket error occurred, retrying (#{attempt} retries already, id: #{movie_id})"
      sleep 0.5
      get_releases_for_id movie_id, attempt: (attempt + 1)
    else
      puts "  !!! A socket error occurred 10 times, cancel retry for movie with id #{movie_id}"
      raise e
    end
  end

  def get_translations_for_id(movie_id, attempt: 0)
    Tmdb::Movie.translations movie_id
  rescue SocketError => e
    if attempt < 10
      puts "  !!! A socket error occurred, retrying (#{attempt} retries already, id: #{movie_id})"
      sleep 0.5
      get_translations_for_id movie_id, attempt: (attempt + 1)
    else
      puts "  !!! A socket error occurred 10 times, cancel retry for movie with id #{movie_id}"
      raise e
    end
  end

  def get_person_for_id(person_id, attempt: 0)
    Tmdb::Person.detail person_id
  rescue SocketError => e
    if attempt < 10
      puts "  !!! A socket error occurred, retrying (#{attempt} retries already, id: #{person_id})"
      sleep 0.5
      get_person_for_id person_id, attempt: (attempt + 1)
    else
      puts "  !!! A socket error occurred 10 times, cancel retry for person with id #{person_id}"
      raise e
    end
  end

  # Note: 'external_ids' is not in spec of ruby gem themoviedb
  def update_person(id, uri_person)
    person = get_person_for_id id

    # try to delete existing triples for person first
    @virtuoso_writer.delete_triple(
        subject: uri_person)

    # add new triples
    @virtuoso_writer.new_triple(
        uri_person, "#{@schemas['rdf']}type", "#{@schemas['tmdb']}Person", literal: false
    )
    @virtuoso_writer.new_triple(
        uri_person, "#{@schemas['tmdb']}person/id", person.id
    )
    @virtuoso_writer.new_triple(
        uri_person, "#{@schemas['tmdb']}person/adult", person.adult
    )
    person.also_known_as.each do |name|
      @virtuoso_writer.new_triple(
          uri_person, "#{@schemas['tmdb']}person/also_known_as", name
      )
    end
    @virtuoso_writer.new_triple(
        uri_person, "#{@schemas['tmdb']}person/biography", person.biography
    )
    @virtuoso_writer.new_triple(
        uri_person, "#{@schemas['tmdb']}person/birthday", person.birthday
    )
    @virtuoso_writer.new_triple(
        uri_person, "#{@schemas['tmdb']}person/deathday", person.deathday
    )
    @virtuoso_writer.new_triple(
        uri_person, "#{@schemas['tmdb']}person/homepage", person.homepage
    )
    @virtuoso_writer.new_triple(
        uri_person, "#{@schemas['tmdb']}person/name", person.name
    )
    @virtuoso_writer.new_triple(
        uri_person, "#{@schemas['tmdb']}person/place_of_birth", person.place_of_birth
    )
    @virtuoso_writer.new_triple(
        uri_person, "#{@schemas['tmdb']}person/profile_path", person.profile_path
    )
    @virtuoso_writer.new_triple(
        uri_person, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
    )

    # credits
    update_credits id, uri_person
  end

  # TODO only credits for movies so far
  def update_credits(person_id, uri_person)
    credits = get_credits_for_person_with_id person_id
    update_casts credits, uri_person
    update_crews credits, uri_person
  end

  def update_casts(credits, uri_person)
    casts = credits['cast']
    casts.each do |cast|
      uri_cast = "#{uri_person}/cast/#{cast['credit_id']}"
      @virtuoso_writer.new_triple(
          uri_person, "#{@schemas['tmdb']}person/combined_credits/cast", uri_cast
      )

      # try to delete existing triples for cast first
      @virtuoso_writer.delete_triple(
          subject: uri_cast)

      # add new triples
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['rdf']}type", "#{@schemas['tmdb']}Combined_Cast"
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}person/combined_credits/cast/adult", cast['adult']
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}person/combined_credits/cast/character", cast['character']
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}person/combined_credits/cast/credit_id", cast['credit_id']
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}person/combined_credits/cast/id", cast['id']
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}person/combined_credits/cast/original_title", cast['original_title']
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}person/combined_credits/cast/poster_path", cast['poster_path']
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}person/combined_credits/cast/release_date", cast['release_date']
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}person/combined_credits/cast/title", cast['title']
      )
      @virtuoso_writer.new_triple(
          uri_cast, "#{@schemas['tmdb']}person/combined_credits/cast/media_type", 'movie'
      )
      @virtuoso_writer.new_triple(
          uri_cast, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      )
    end
  end

  def update_crews(credits, uri_person)
    crews = credits['crew']
    crews.each do |crew|
      uri_crew = "#{uri_person}/cast/#{crew['credit_id']}"
      @virtuoso_writer.new_triple(
          uri_person, "#{@schemas['tmdb']}person/combined_credits/crew", uri_crew
      )

      # try to delete existing triples for crew first
      @virtuoso_writer.delete_triple(
          subject: uri_crew)

      # add new triples
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}person/combined_credits/crew/adult", crew['adult']
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}person/combined_credits/crew/credit_id", crew['credit_id']
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}person/combined_credits/crew/id", crew['id']
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}person/combined_credits/crew/department", crew['department']
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}person/combined_credits/crew/job", crew['job']
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}person/combined_credits/crew/original_title", crew['original_title']
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}person/combined_credits/crew/poster_path", crew['poster_path']
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}person/combined_credits/crew/release_date", crew['release_date']
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}person/combined_credits/crew/title", crew['title']
      )
      @virtuoso_writer.new_triple(
          uri_crew, "#{@schemas['tmdb']}person/combined_credits/crew/media_type", 'movie'
      )
      @virtuoso_writer.new_triple(
          uri_crew, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      )
    end
  end

  def get_credits_for_person_with_id(person_id, attempt: 0)
    Tmdb::Person.credits person_id
  rescue SocketError => e
    if attempt < 10
      puts "  !!! A socket error occurred, retrying (#{attempt} retries already, id: #{person_id})"
      sleep 0.5
      get_credits_for_person_with_id person_id, attempt: (attempt + 1)
    else
      puts "  !!! A socket error occurred 10 times, cancel retry for movie with id #{person_id}"
      raise e
    end
  end

  def set_xsd_type(literal, type)
    RDF::Literal.new(literal, datatype: "http://www.w3.org/2001/XMLSchema##{type}")
  end

  private
  def secrets
    @secrets ||= YAML.load_file '../config/secrets.yml'
  end

  def initialize_tmdb_api
    Tmdb::Api.key secrets['services']['tmdb']['api_key']
  end

  private
  def load_schemas
    file ||= YAML.load_file '../config/namespaces.yml'
    @schemas = file['schemas']
  end
end