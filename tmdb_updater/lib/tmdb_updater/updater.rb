require 'yaml'
require 'themoviedb'

class TMDbUpdater::Updater
  def initialize
    initialize_tmdb_api
  end

  def register_receiver
    @receiver = TMDbUpdater::MsgConsumer.new
    @receiver.subscribe(type: :movie_id) { |movie_id| update_movie(movie_id) }
  end

  def init_virtuoso
    @virtuoso = TMDbUpdater::VirtuosoWriter.new
  end

  # TODO what if triple already in database? overwrite?
  def update_movie(movie_id)
    base = "http://www.hpi.uni-potsdam.de/semmul2014/"
    tmdb = "http://www.hpi.uni-potsdam.de/semmul2014/themoviedb.owl#"
    rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    xsd = "http://www.w3.org/2001/XMLSchema#"

    movie = get_movie_with_id movie_id

    # all data about movie with movie_id
    movie_details = movie.detail
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      predicate:"#{rdf}type",
      object:"#{tmdb}Movie", literal: false)
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      predicate:"#{tmdb}movie/id",
      object:"#{movie_details.id}^^#{xsd}int")
    @virtuoso.new_triple(\
      subject:"#{base}movie/#{movie_id}",
      predicate:"#{tmdb}movie/adult",
      object:"#{movie_details.adult}^^#{xsd}boolean")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      predicate:"#{tmdb}movie/backdrop_path",
      object:"#{movie_details.backdrop_path}")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      predicate:"#{tmdb}movie/budget",
      object:"#{movie_details.budget}^^#{xsd}int")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      predicate:"#{tmdb}movie/homepage",
      object:"#{movie_details.homepage}")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      predicate:"#{tmdb}movie/imdb_id",
      object:"#{movie_details.imdb_id}")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      predicate:"#{tmdb}movie/original_title",
      object:"#{movie_details.original_title}")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      predicate:"#{tmdb}movie/overview",
      object:"#{movie_details.overview}")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      predicate:"#{tmdb}movie/popularity",
      object:"#{movie_details.popularity}^^#{xsd}decimal")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      predicate:"#{tmdb}movie/poster_path",
      object:"#{movie_details.poster_path}")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      predicate:"#{tmdb}movie/release_date",
      object:"#{movie_details.release_date}")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      predicate:"#{tmdb}movie/revenue",
      object:"#{movie_details.revenue}^^#{xsd}int")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      prhedicate:"#{tmdb}movie/runtime",
      object:"#{movie_details.runtime}^^#{xsd}int")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      prhedicate:"#{tmdb}movie/status",
      object:"#{movie_details.status}")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      prhedicate:"#{tmdb}movie/tagline",
      object:"#{movie_details.tagline}")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      prhedicate:"#{tmdb}movie/title",
      object:"#{movie_details.title}")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      prhedicate:"#{tmdb}movie/vote_average",
      object:"#{movie_details.vote_average}^^#{xsd}decimal")
    @virtuoso.new_triple(
      subject:"#{base}movie/#{movie_id}",
      prhedicate:"#{tmdb}movie/vote_count",
      object:"#{movie_details.vote_count}^^#{xsd}int")

    # alternative titles
    movie_titles = movie.alternative_titles['titles']   # id is ignored because its identical with movie_id
    movie_titles.each do |title|
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}",
        predicate:"#{tmdb}movie/alternative_titles/titles",
        object:"#{base}movie/#{movie_id}/alternative_titles/#{title.iso_3166_1}", literal: false)
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/alternative_titles/#{title.iso_3166_1}",
        predicate:"#{rdf}type",
        object:"#{tmdb}Alternative_Title", literal: false)
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/alternative_titles/#{title.iso_3166_1}",
        predicate:"#{tmdb}movie/alternative_titles/titles/iso_3166_1",
        object:"#{title.iso_3166_1}")
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/alternative_titles/#{title.iso_3166_1}",
        predicate:"#{tmdb}movie/alternative_titles/titles/title",
        object:"#{title.title}")
    end

    # cast
    movie_casts = movie.casts
    movie_casts.each do |cast|
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}",
        predicate:"#{tmdb}movie/credits/cast",
        object:"#{base}movie/#{movie_id}/cast/#{cast['id']}", literal: false)
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/cast/#{cast['id']}",
        predicate:"#{rdf}type",
        object:"#{tmdb}Cast", literal: false)
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/cast/#{cast['id']}",
        predicate:"#{tmdb}cast/id",
        object:"#{cast['id']}^^#{xsd}int")
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/cast/#{cast['id']}",
        predicate:"#{tmdb}cast/cast_id",
        object:"#{cast['cast_id']}^^#{xsd}int")
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/cast/#{cast['id']}",
        predicate:"#{tmdb}cast/character",
        object:"#{cast['character']}")
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/cast/#{cast['id']}",
        predicate:"#{tmdb}cast/name",
        object:"#{cast['name']}")
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/cast/#{cast['id']}",
        predicate:"#{tmdb}cast/order",
        object:"#{cast['order']}")
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/cast/#{cast['id']}",
        predicate:"#{tmdb}cast/profile_path",
        object:"#{cast['profile_path']}")
    end

    # crew
    movie_crew = movie.crew
    movie_crew.each do |crew|
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}",
        predicate:"#{tmdb}movie/credits/crew",
        object:"#{base}movie/#{movie_id}/crew/#{crew['id']}", literal: false)
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/crew/#{crew['id']}",
        predicate:"#{rdf}type",
        object:"#{tmdb}Crew", literal: false)
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/crew/#{crew['id']}",
        predicate:"#{tmdb}crew/id",
        object:"#{crew['id']}^^#{xsd}int")
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/crew/#{crew['id']}",
        predicate:"#{tmdb}crew/department",
        object:"#{crew['department']}")
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/crew/#{crew['id']}",
        predicate:"#{tmdb}crew/job",
        object:"#{crew['job']}")
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/crew/#{crew['id']}",
        predicate:"#{tmdb}crew/name",
        object:"#{crew['name']}")
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/crew/#{crew['id']}",
        predicate:"#{tmdb}crew/profile_path",
        object:"#{crew['profile_path']}")
    end

    # collection
    collection_id = movie_details.belongs_to_collection
    if collection_id
      puts collection_id
      collection = get_collection collection_id
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}",
        predicate:"#{tmdb}movie/belongs_to_collection",
        object:"#{base}collection/#{collection_id}", literal: false)
      # TODO check if collection is already in database
      @virtuoso.new_triple(
        subject:"#{base}collection/#{collection_id}",
        predicate:"#{rdf}type",
        object:"#{tmdb}Collection", literal: false)
      @virtuoso.new_triple(
        subject:"#{base}collection/#{collection_id}",
        predicate:"#{tmdb}movie/belongs_to_collection/id",
        object:"#{collection_id}^^#{xsd}int")
      @virtuoso.new_triple(
        subject:"#{base}collection/#{collection_id}",
        predicate:"#{tmdb}movie/belongs_to_collection/name",
        object:"#{collection.name}")
      @virtuoso.new_triple(
        subject:"#{base}collection/#{collection_id}",
        predicate:"#{tmdb}movie/belongs_to_collection/poster_path",
        object:"#{collection.poster_path}")
      @virtuoso.new_triple(
        subject:"#{base}collection/#{collection_id}",
        predicate:"#{tmdb}movie/belongs_to_collection/backdrop_path",
        object:"#{collection.backdrop_path}")
    end

    # genres
    movie_genres = movie_details.genres
    movie_genres.each do |genre|
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}",
        predicate:"#{tmdb}movie/genres",
        object:"#{base}genre/#{genre['id']}", literal: false)
      # TODO check if genre is already in database
      @virtuoso.new_triple(
        subject:"#{base}genre/#{genre['id']}",
        predicate:"#{rdf}type",
        object:"#{tmdb}Genre", literal: false)
      @virtuoso.new_triple(
        subject:"#{base}genre/#{genre['id']}",
        predicate:"#{tmdb}genres/id",
        object:"#{genre['id']}^^#{xsd}int")
      @virtuoso.new_triple(
        subject:"#{base}genre/#{genre['id']}",
        predicate:"#{tmdb}genres/name",
        object:"#{genre['name']}")
    end

    # production companies
    movie_companies = movie_details.production_companies
    movie_companies.each do |company|
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}",
        predicate:"#{tmdb}movie/production_companies",
        object:"#{base}company/#{company['id']}", literal: false)
      # TODO check if company is already in database
      @virtuoso.new_triple(
        subject:"##{base}company/#{company['id']}",
        predicate:"#{rdf}type",
        object:"#{tmdb}Production_Company", literal: false)
      @virtuoso.new_triple(
        subject:"##{base}company/#{company['id']}",
        predicate:"#{tmdb}movie/production_companies/id",
        object:"#{company['id']}^^#{xsd}int")
      @virtuoso.new_triple(\
        subject:"##{base}company/#{company['id']}", \
        predicate:"#{tmdb}movie/production_companies/name", \
        object:"#{company['name']}")
    end

    # production countries
    movie_countries = movie_details.production_countries
    movie_countries.each do |country|
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}",
        predicate:"#{tmdb}movie/production_countries",
        object:"#{base}country/#{country['iso_639_1']}", literal: false)
      # TODO check if country is already in database
      @virtuoso.new_triple(
        subject:"#{base}country/#{country['iso_639_1']}",
        predicate:"#{rdf}type",
        object:"#{tmdb}Production_Country", literal: false)
      @virtuoso.new_triple(
        subject:"#{base}country/#{country['iso_639_1']}",
        predicate:"#{tmdb}movie/production_countries/iso_639_1",
        object:"#{country['iso_639_1']}")
      @virtuoso.new_triple(
        subject:"#{base}country/#{country['iso_639_1']}",
        predicate:"#{tmdb}movie/production_countries/name",
        object:"#{country['name']}")
    end

    # releases
    movie_releases = movie.releases['countries']
    movie_releases.each do |release|
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}",
        predicate:"#{tmdb}movie/releases/countries",
        object:"#{base}movie/#{movie_id}/releases/#{release}['iso_3166_1']", literal: false)
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/releases/#{release}['iso_3166_1']",
        predicate:"#{rdf}type",
        object:"#{tmdb}Release_Country", literal: false)
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/releases/#{release}['iso_3166_1']",
        predicate:"#{tmdb}movie/releases/countries/iso_3166_1",
        object:"#{release['iso_3166_1']}")
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/releases/#{release}['iso_3166_1']",
        predicate:"#{tmdb}movie/releases/countries/certification",
        object:"#{release['certification']}")
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}/releases/#{release}['iso_3166_1']",
        predicate:"#{tmdb}movie/releases/countries/release_date",
        object:"#{release['release_date']}")
    end

    # spoken languages
    movie_languages = movie_details.spoken_languages
    movie_languages.each do |language|
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}",
        predicate:"#{tmdb}movie/spoken_languages",
        object:"#{base}language/#{language['iso_639_1']}", literal: false)
      # TODO check if language is already in database
      @virtuoso.new_triple(
        subject:"#{base}language/#{language['iso_639_1']}",
        predicate:"#{rdf}type",
        object:"#{tmdb}Spoken_Language", literal: false)
      @virtuoso.new_triple(
        subject:"#{base}language/#{language['iso_639_1']}",
        predicate:"#{tmdb}movie/spoken_languages/iso_639_1",
        object:"#{language['iso_639_1']}")
      @virtuoso.new_triple(
        subject:"#{base}language/#{language['iso_639_1']}",
        predicate:"#{tmdb}movie/spoken_languages/name",
        object:"#{language['name']}")
    end

    # translations
    movie_translations = movie.translations['translations']
    movie_translations.each do |translation|
      @virtuoso.new_triple(
        subject:"#{base}movie/#{movie_id}",
        predicate:"#{tmdb}movie/translations/translations",
        object:"#{base}translation/#{translation['iso_639_1']}", literal: false)
      # TODO check if translation is already in database
      @virtuoso.new_triple(
        subject:"#{base}translation/#{translation['iso_639_1']}",
        predicate:"#{rdf}type",
        object:"#{tmdb}Translation", literal: false)
      @virtuoso.new_triple(
        subject:"#{base}translation/#{translation['iso_639_1']}",
        predicate:"#{tmdb}movie/translations/translations/iso_639_1",
        object:"#{translation['iso_639_1']}")
      @virtuoso.new_triple(
        subject:"#{base}translation/#{translation['iso_639_1']}",
        predicate:"#{tmdb}movie/translations/translations/name",
        object:"#{translation['name']}")
      @virtuoso.new_triple(
        subject:"#{base}translation/#{translation['iso_639_1']}",
        predicate:"#{tmdb}movie/translations/translations/english_name",
        object:"#{translation['english_name']}")
    end

  end

  def get_movie_for_id(movie_id, attempt: 0)
    Tmdb::Movie movie_id
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

  def get_collection(collection_id, attempt: 0)
    Tmdb::Collection.detail collection_id
  rescue SocketError => e
    if attempt < 10
      puts "  !!! A socket error occurred, retrying (#{attempt} retries already, id: #{collection_id})"
      sleep 0.5
      get_collection collection_id, attempt: (attempt + 1)
    else
      puts "  !!! A socket error occurred 10 times, cancel retry for movie with id #{collection_id}"
      raise e
    end
  end

  private
  def secrets
    @secrets ||= YAML.load_file '../config/secrets.yml'
  end

  def initialize_tmdb_api
    Tmdb::Api.key secrets['services']['tmdb']['api_key']
  end
end
