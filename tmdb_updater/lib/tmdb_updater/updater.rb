require 'yaml'
require 'themoviedb'

class TMDbUpdater::Updater

  BASE_NAMESPACE = 'http://www.hpi.uni-potsdam.de/semmul2014/raw/tmdb/'
  TMDB_NAMESPACE = 'http://www.hpi.uni-potsdam.de/semmul2014/themoviedb.owl#'
  RDF_NAMESPACE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
  XSD_NAMESPACE = 'http://www.w3.org/2001/XMLSchema#'
  PAV_LASTUPDATEON = 'http://purl.org/pav/lastUpdateOn'

  def initialize
    @publisher = TMDbUpdater::MsgPublisher.new
    @virtuoso = TMDbUpdater::VirtuosoWriter.new
    initialize_tmdb_api
  end

  def register_receiver
    @receiver = TMDbUpdater::MsgConsumer.new
    @receiver.subscribe(type: :movie_id) { |movie_id| update(movie_id) }
  end

  # TODO what if triple already in database? overwrite?
  def update(movie_id)
    # all data about movie with movie_id
    movie_details = get_movie_for_id movie_id
    uri_movie = "#{BASE_NAMESPACE}movie/#{movie_id}"
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
    @publisher.enqueue_uri :movie_uri, uri_movie
  end
  
  def update_movie(movie, uri_movie)
    # try to delete existing triples for movie first
    @virtuoso.delete_triple(
        subject: uri_movie)

    # add new triples
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{RDF_NAMESPACE}type",
        object: "#{TMDB_NAMESPACE}Movie", literal: false)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/id",
        #object: (set_xsd_type movie.id, 'int'))
        object: movie.id)
    @virtuoso.new_triple(\
      subject: uri_movie,
      predicate: "#{TMDB_NAMESPACE}movie/adult",
      object: (set_xsd_type movie.adult, 'boolean'))
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/backdrop_path",
        object: movie.backdrop_path)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/budget",
        #object: (set_xsd_type movie.budget, 'int'))
        object: movie.budget)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/homepage",
        object: movie.homepage)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/imdb_id",
        object: movie.imdb_id)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/original_title",
        object: movie.original_title)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/overview",
        object: movie.overview)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/popularity",
        object: (set_xsd_type movie.popularity, 'decimal'))
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/poster_path",
        object: movie.poster_path)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/release_date",
        object: movie.release_date)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/revenue",
        #object: (set_xsd_type movie.revenue, 'int'))
        object: movie.revenue)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/runtime",
        #object: (set_xsd_type movie.runtime, 'int'))
        object: movie.runtime)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/status",
        object: movie.status)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/tagline",
        object: movie.tagline)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/title",
        object: movie.title)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/vote_average",
        object: (set_xsd_type movie.vote_average, 'decimal'))
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: "#{TMDB_NAMESPACE}movie/vote_count",
        #object: (set_xsd_type movie.vote_count, 'int'))
        object: movie.vote_count)
    @virtuoso.new_triple(
        subject: uri_movie,
        predicate: PAV_LASTUPDATEON,
        object: (set_xsd_type DateTime.now, 'dateTime'))
  end
  
  def update_titles(titles, uri_movie)
    movie_titles = titles['titles']   # id is ignored because its identical with movie_id
    movie_titles.each do |title|
      uri_title = "#{uri_movie}/alternative_titles/#{title['iso_3166_1']}"
      @virtuoso.new_triple(
          subject: uri_movie,
          predicate: "#{TMDB_NAMESPACE}movie/alternative_titles/titles",
          object: uri_title, literal: false)

      # try to delete existing triples for title first
      @virtuoso.delete_triple(
          subject: uri_title)

      # add new triples
      @virtuoso.new_triple(
          subject: uri_title,
          predicate: "#{RDF_NAMESPACE}type",
          object: "#{TMDB_NAMESPACE}Alternative_Title", literal: false)
      @virtuoso.new_triple(
          subject: uri_title,
          predicate: "#{TMDB_NAMESPACE}movie/alternative_titles/titles/iso_3166_1",
          object: title['iso_3166_1'])
      @virtuoso.new_triple(
          subject: uri_title,
          predicate: "#{TMDB_NAMESPACE}movie/alternative_titles/titles/title",
          object: title['title'])
      @virtuoso.new_triple(
          subject: uri_title,
          predicate: PAV_LASTUPDATEON,
          object: (set_xsd_type DateTime.now, 'dateTime'))
    end
  end

  def update_cast(movie_casts, uri_movie)
    movie_casts.each do |cast|
      uri_cast = "#{uri_movie}/cast/#{cast['id']}"
      @virtuoso.new_triple(
          subject: uri_movie,
          predicate: "#{TMDB_NAMESPACE}movie/credits/cast",
          object: uri_cast, literal: false)

      # try to delete existing triples for cast first
      @virtuoso.delete_triple(
          subject: uri_cast)

      # add new triples
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{RDF_NAMESPACE}type",
          object: "#{TMDB_NAMESPACE}Cast", literal: false)
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}cast/id",
          #object: (set_xsd_type cast['id'], 'int'))
          object: cast['id'])
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}cast/cast_id",
          #object: (set_xsd_type cast['cast_id'], 'int'))
          object: cast['cast_id'])
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}cast/character",
          object: cast['character'])
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}cast/name",
          object: cast['name'])
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}cast/order",
          object: cast['order'])
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}cast/profile_path",
          object: cast['profile_path'])
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: PAV_LASTUPDATEON,
          object: (set_xsd_type DateTime.now, 'dateTime'))
      # update person
      update_person cast['id']
    end
  end
  
  def update_crew(movie_crew, uri_movie)
    movie_crew.each do |crew|
      uri_crew = "#{uri_movie}/crew/#{crew['id']}"
      @virtuoso.new_triple(
          subject: uri_movie,
          predicate: "#{TMDB_NAMESPACE}movie/credits/crew",
          object: uri_crew, literal: false)

      # try to delete existing triples for crew first
      @virtuoso.delete_triple(
          subject: uri_crew)

      # add new triples
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{RDF_NAMESPACE}type",
          object: "#{TMDB_NAMESPACE}Crew", literal: false)
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}crew/id",
          #object: (set_xsd_type crew['id'], 'int'))
          object: crew['id'])
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}crew/department",
          object: crew['department'])
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}crew/job",
          object: crew['job'])
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}crew/name",
          object: crew['name'])
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}crew/profile_path",
          object: crew['profile_path'])
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: PAV_LASTUPDATEON,
          object: (set_xsd_type DateTime.now, 'dateTime'))
      # update person
      update_person crew['id']
    end
  end
  
  def update_collection(movie, uri_movie)
    collection_id = movie.belongs_to_collection['id'] if movie.belongs_to_collection
    if collection_id
      collection = get_collection_for_id collection_id
      uri_collection = "#{BASE_NAMESPACE}collection/#{collection_id}"
      @virtuoso.new_triple(
          subject: uri_movie,
          predicate: "#{TMDB_NAMESPACE}movie/belongs_to_collection",
          object: uri_collection, literal: false)

      # try to delete existing triples for collection first
      @virtuoso.delete_triple(
          subject: uri_collection)

      # add new triples
      @virtuoso.new_triple(
          subject: uri_collection,
          predicate: "#{RDF_NAMESPACE}type",
          object: "#{TMDB_NAMESPACE}Collection", literal: false)
      @virtuoso.new_triple(
          subject: uri_collection,
          predicate: "#{TMDB_NAMESPACE}movie/belongs_to_collection/id",
          #object: (set_xsd_type collection_id, 'int'))
          object: collection_id)
      @virtuoso.new_triple(
          subject: uri_collection,
          predicate: "#{TMDB_NAMESPACE}movie/belongs_to_collection/name",
          object: collection.name)
      @virtuoso.new_triple(
          subject: uri_collection,
          predicate: "#{TMDB_NAMESPACE}movie/belongs_to_collection/poster_path",
          object: collection.poster_path)
      @virtuoso.new_triple(
          subject: uri_collection,
          predicate: "#{TMDB_NAMESPACE}movie/belongs_to_collection/backdrop_path",
          object: collection.backdrop_path)
      @virtuoso.new_triple(
          subject: uri_collection,
          predicate: PAV_LASTUPDATEON,
          object: (set_xsd_type DateTime.now, 'dateTime'))
    end
  end
  
  def update_genres(movie, uri_movie)
    movie_genres = movie.genres
    movie_genres.each do |genre|
      uri_genre = "#{BASE_NAMESPACE}genre/#{genre['id']}"
      @virtuoso.new_triple(
          subject: uri_movie,
          predicate: "#{TMDB_NAMESPACE}movie/genres",
          object: uri_genre, literal: false)

      # try to delete existing triples for genre first
      @virtuoso.delete_triple(
          subject: uri_genre)

      # add new triples
      @virtuoso.new_triple(
          subject: uri_genre,
          predicate: "#{RDF_NAMESPACE}type",
          object: "#{TMDB_NAMESPACE}Genre", literal: false)
      @virtuoso.new_triple(
          subject: uri_genre,
          predicate: "#{TMDB_NAMESPACE}genres/id",
          #object: (set_xsd_type genre['id'], 'int'))
          object: genre['id'])
      @virtuoso.new_triple(
          subject: uri_genre,
          predicate: "#{TMDB_NAMESPACE}genres/name",
          object: genre['name'])
      @virtuoso.new_triple(
          subject: uri_genre,
          predicate: PAV_LASTUPDATEON,
          object: (set_xsd_type DateTime.now, 'dateTime'))
    end
  end
  
  def update_companies(movie, uri_movie)
    movie_companies = movie.production_companies
    movie_companies.each do |company|
      uri_company = "#{BASE_NAMESPACE}company/#{company['id']}"
      @virtuoso.new_triple(
          subject: uri_movie,
          predicate: "#{TMDB_NAMESPACE}movie/production_companies",
          object: uri_company, literal: false)

      # try to delete existing triples for company first
      @virtuoso.delete_triple(
          subject: uri_company)

      # add new triples
      @virtuoso.new_triple(
          subject: uri_company,
          predicate: "#{RDF_NAMESPACE}type",
          object: "#{TMDB_NAMESPACE}Production_Company", literal: false)
      @virtuoso.new_triple(
          subject: uri_company,
          predicate: "#{TMDB_NAMESPACE}movie/production_companies/id",
          #object: (set_xsd_type company['id'], 'int'))
          object: company['id'])
      @virtuoso.new_triple(
        subject: uri_company,
        predicate: "#{TMDB_NAMESPACE}movie/production_companies/name",
        object: company['name'])
      @virtuoso.new_triple(
          subject: uri_company,
          predicate: PAV_LASTUPDATEON,
          object: (set_xsd_type DateTime.now, 'dateTime'))
    end
  end
  
  def update_countries(movie, uri_movie)
    movie_countries = movie.production_countries
    movie_countries.each do |country|
      uri_country = "#{BASE_NAMESPACE}country/#{country['iso_639_1']}"
      @virtuoso.new_triple(
          subject: uri_movie,
          predicate: "#{TMDB_NAMESPACE}movie/production_countries",
          object: uri_country, literal: false)

      # try to delete existing triples for country first
      @virtuoso.delete_triple(
          subject: uri_country)

      # add new triples
      @virtuoso.new_triple(
          subject: uri_country,
          predicate: "#{RDF_NAMESPACE}type",
          object: "#{TMDB_NAMESPACE}Production_Country", literal: false)
      @virtuoso.new_triple(
          subject: uri_country,
          predicate: "#{TMDB_NAMESPACE}movie/production_countries/iso_639_1",
          object: country['iso_639_1'])
      @virtuoso.new_triple(
          subject: uri_country,
          predicate: "#{TMDB_NAMESPACE}movie/production_countries/name",
          object: country['name'])
      @virtuoso.new_triple(
          subject: uri_country,
          predicate: PAV_LASTUPDATEON,
          object: (set_xsd_type DateTime.now, 'dateTime'))
    end
  end
  
  def update_releases(releases, uri_movie)
    movie_releases = releases['countries']
    movie_releases.each do |release|
      uri_release = "#{uri_movie}/releases/#{release['iso_3166_1']}"
      @virtuoso.new_triple(
          subject: uri_movie,
          predicate: "#{TMDB_NAMESPACE}movie/releases/countries",
          object: uri_release, literal: false)

      # try to delete existing triples for release first
      @virtuoso.delete_triple(
          subject: uri_release)

      # add new triples
      @virtuoso.new_triple(
          subject: uri_release,
          predicate: "#{RDF_NAMESPACE}type",
          object: "#{TMDB_NAMESPACE}Release_Country", literal: false)
      @virtuoso.new_triple(
          subject: uri_release,
          predicate: "#{TMDB_NAMESPACE}movie/releases/countries/iso_3166_1",
          object: release['iso_3166_1'])
      @virtuoso.new_triple(
          subject: uri_release,
          predicate: "#{TMDB_NAMESPACE}movie/releases/countries/certification",
          object: release['certification'])
      @virtuoso.new_triple(
          subject: uri_release,
          predicate: "#{TMDB_NAMESPACE}movie/releases/countries/release_date",
          object: release['release_date'])
      @virtuoso.new_triple(
          subject: uri_release,
          predicate: PAV_LASTUPDATEON,
          object: (set_xsd_type DateTime.now, 'dateTime'))
    end
  end

  def update_languages(movie_languages, uri_movie)
    movie_languages.each do |language|
      uri_language = "#{BASE_NAMESPACE}language/#{language['iso_639_1']}"
      @virtuoso.new_triple(
          subject: uri_movie,
          predicate: "#{TMDB_NAMESPACE}movie/spoken_languages",
          object: uri_language, literal: false)

      # try to delete existing triples for language first
      @virtuoso.delete_triple(
          subject: uri_language)

      # add new triples
      @virtuoso.new_triple(
          subject: uri_language,
          predicate: "#{RDF_NAMESPACE}type",
          object: "#{TMDB_NAMESPACE}Spoken_Language", literal: false)
      @virtuoso.new_triple(
          subject: uri_language,
          predicate: "#{TMDB_NAMESPACE}movie/spoken_languages/iso_639_1",
          object: language['iso_639_1'])
      @virtuoso.new_triple(
          subject: uri_language,
          predicate: "#{TMDB_NAMESPACE}movie/spoken_languages/name",
          object: language['name'])
      @virtuoso.new_triple(
          subject: uri_language,
          predicate: PAV_LASTUPDATEON,
          object: (set_xsd_type DateTime.now, 'dateTime'))
    end
  end

  def update_translations(translations, uri_movie)
    movie_translations = translations['translations']
    movie_translations.each do |translation|
      uri_translation = "#{BASE_NAMESPACE}translation/#{translation['iso_639_1']}"
      @virtuoso.new_triple(
          subject: uri_movie,
          predicate: "#{TMDB_NAMESPACE}movie/translations/translations",
          object: uri_translation, literal: false)

      # try to delete existing triples for translation first
      @virtuoso.delete_triple(
          subject: uri_translation)

      # add new triples
      @virtuoso.new_triple(
          subject: uri_translation,
          predicate: "#{RDF_NAMESPACE}type",
          object: "#{TMDB_NAMESPACE}Translation", literal: false)
      @virtuoso.new_triple(
          subject: uri_translation,
          predicate: "#{TMDB_NAMESPACE}movie/translations/translations/iso_639_1",
          object: translation['iso_639_1'])
      @virtuoso.new_triple(
          subject: uri_translation,
          predicate: "#{TMDB_NAMESPACE}movie/translations/translations/name",
          object: translation['name'])
      @virtuoso.new_triple(
          subject: uri_translation,
          predicate: "#{TMDB_NAMESPACE}movie/translations/translations/english_name",
          object: translation['english_name'])
      @virtuoso.new_triple(
          subject: uri_translation,
          predicate: PAV_LASTUPDATEON,
          object: (set_xsd_type DateTime.now, 'dateTime'))
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
  def update_person(id)
    person = get_person_for_id id
    uri_person = "#{BASE_NAMESPACE}person/#{id}"

    # try to delete existing triples for person first
    @virtuoso.delete_triple(
        subject: uri_person)

    # add new triples
    @virtuoso.new_triple(
        subject: uri_person,
        predicate: "#{RDF_NAMESPACE}type",
        object: "#{TMDB_NAMESPACE}Person", literal: false)
    @virtuoso.new_triple(
        subject: uri_person,
        predicate: "#{TMDB_NAMESPACE}person/id",
        #object: (set_xsd_type person.id, 'int'))
        object: person.id)
    @virtuoso.new_triple(
        subject: uri_person,
        predicate: "#{TMDB_NAMESPACE}person/adult",
        object: (set_xsd_type person.adult, 'boolean'))
    person.also_known_as.each do |name|
      @virtuoso.new_triple(
          subject: uri_person,
          predicate: "#{TMDB_NAMESPACE}person/also_known_as",
          object: name)
    end
    @virtuoso.new_triple(
        subject: uri_person,
        predicate: "#{TMDB_NAMESPACE}person/biography",
        object: person.biography)
    @virtuoso.new_triple(
        subject: uri_person,
        predicate: "#{TMDB_NAMESPACE}person/birthday",
        object: person.birthday)
    @virtuoso.new_triple(
        subject: uri_person,
        predicate: "#{TMDB_NAMESPACE}person/deathday",
        object: person.deathday)
    @virtuoso.new_triple(
        subject: uri_person,
        predicate: "#{TMDB_NAMESPACE}person/homepage",
        object: person.homepage)
    @virtuoso.new_triple(
        subject: uri_person,
        predicate: "#{TMDB_NAMESPACE}person/name",
        object: person.name)
    @virtuoso.new_triple(
        subject: uri_person,
        predicate: "#{TMDB_NAMESPACE}person/place_of_birth",
        object: person.place_of_birth)
    @virtuoso.new_triple(
        subject: uri_person,
        predicate: "#{TMDB_NAMESPACE}person/profile_path",
        object: person.profile_path)
    @virtuoso.new_triple(
        subject: uri_person,
        predicate: PAV_LASTUPDATEON,
        object: (set_xsd_type DateTime.now, 'dateTime'))

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
      @virtuoso.new_triple(
          subject: uri_person,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/cast",
          object: uri_cast)

      # try to delete existing triples for cast first
      @virtuoso.delete_triple(
          subject: uri_cast)

      # add new triples
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{RDF_NAMESPACE}type",
          object: "#{TMDB_NAMESPACE}Combined_Cast")
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/cast/adult",
          object: (set_xsd_type cast['adult'], 'boolean'))
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/cast/character",
          object: cast['character'])
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/cast/credit_id",
          object: cast['credit_id'])
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/cast/id",
          #object: (set_xsd_type cast['id'], 'int'))
          object: cast['id'])
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/cast/original_title",
          object: cast['original_title'])
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/cast/poster_path",
          object: cast['poster_path'])
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/cast/release_date",
          object: cast['release_date'])
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/cast/title",
          object: cast['title'])
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/cast/media_type",
          object: 'movie')
      @virtuoso.new_triple(
          subject: uri_cast,
          predicate: PAV_LASTUPDATEON,
          object: (set_xsd_type DateTime.now, 'dateTime'))
    end
  end

  def update_crews(credits, uri_person)
    crews = credits['crew']
    crews.each do |crew|
      uri_crew = "#{uri_person}/cast/#{crew['credit_id']}"
      @virtuoso.new_triple(
          subject: uri_person,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/crew",
          object: uri_crew)

      # try to delete existing triples for crew first
      @virtuoso.delete_triple(
          subject: uri_crew)

      # add new triples
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/crew/adult",
          object: (set_xsd_type crew['adult'], 'boolean'))
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/crew/credit_id",
          object: crew['credit_id'])
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/crew/id",
          #object: (set_xsd_type crew['id'], 'int'))
          object: crew['id'])
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/crew/department",
          object: crew['department'])
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/crew/job",
          object: crew['job'])
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/crew/original_title",
          object: crew['original_title'])
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/crew/poster_path",
          object: crew['poster_path'])
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/crew/release_date",
          object: crew['release_date'])
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/crew/title",
          object: crew['title'])
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: "#{TMDB_NAMESPACE}person/combined_credits/crew/media_type",
          object: 'movie')
      @virtuoso.new_triple(
          subject: uri_crew,
          predicate: PAV_LASTUPDATEON,
          object: (set_xsd_type DateTime.now, 'dateTime'))
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
    "#{literal}^^#{XSD_NAMESPACE}#{type}"
  end

  private
  def secrets
    @secrets ||= YAML.load_file 'config/secrets.yml'
  end

  def initialize_tmdb_api
    Tmdb::Api.key secrets['services']['tmdb']['api_key']
  end
end