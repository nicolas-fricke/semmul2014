require 'time'
require 'yaml'

class TMDbMapper::Mapper
  BASE_NAMESPACE    = 'http://www.hpi.uni-potsdam.de/semmul2014/mapped/tmdb/'
  TMDB_NAMESPACE    = 'http://www.hpi.uni-potsdam.de/semmul2014/themoviedb.owl#'
  LOM_NAMESPACE     = 'http://www.hpi.uni-potsdam.de/semmul2014/lodofmovies.owl#'
  SCHEMA_NAMESPACE  = 'http://schema.org/'
  DBPEDIA_NAMESPACE = 'http://dbpedia.org/ontology/'
  RDF_NAMESPACE     = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
  RDFS_NAMESPACE    = 'http://www.w3.org/2000/01/rdf-schema#'
  XSD_NAMESPACE     = 'http://www.w3.org/2001/XMLSchema#'

  #TODO move NAMESPACES to extra file (so that we do not have to define them in each class)

  def initialize
    @publisher = TMDbMapper::MsgPublisher.new
    @virtuoso_writer = TMDbMapper::VirtuosoWriter.new
    @virtuoso_reader = TMDbMapper::VirtuosoReader.new
  end
  
  def register_receiver
    @receiver = TMDbMapper::MsgConsumer.new
    @receiver.subscribe(type: :movie_uri) { |movie_uri| map(movie_uri) }
  end

  def map(raw_db_uri)
    @publisher.enqueue_uri :movie_uri, raw_db_uri
    @virtuoso_writer.new_triple raw_db_uri, "#{RDF_NAMESPACE}type", "#{LOM_NAMESPACE}Movie"
    map_movie_titles(raw_db_uri)
    map_movie_release_dates(raw_db_uri)
    map_movie_production_companies(raw_db_uri)
    map_cast(raw_db_uri)
  end

  def map_movie_titles(raw_db_uri)
    titles = @virtuoso_reader.get_objects_for(
      subject: raw_db_uri,
      predicate: "#{TMDB_NAMESPACE}movie/title"
    )
    titles.each do |title|
      @virtuoso_writer.new_triple(
          raw_db_uri, "#{SCHEMA_NAMESPACE}name", title
      )
      # puts title.to_s
    end if titles
  end

  def map_movie_release_dates(raw_db_uri)
    dates = @virtuoso_reader.get_objects_for(
      subject: raw_db_uri,
      predicate: "#{TMDB_NAMESPACE}movie/release_date"
    )
    dates.each do |release_date|
      begin
        date_string = Date.parse(release_date.to_s).xmlschema
        @virtuoso_writer.new_triple(
            raw_db_uri, "#{SCHEMA_NAMESPACE}datePublished", "#{date_string}^^#{XSD_NAMESPACE}date"
        )
        # puts date_string
      rescue ArgumentError
        puts "Could not parse release date `#{release_date.to_s}' as date."
      end
    end if dates
  end

  def map_movie_production_companies(raw_db_uri)
    companies = @virtuoso_reader.get_objects_for(
        subject: raw_db_uri,
        predicate: "#{TMDB_NAMESPACE}movie/production_companies"
    )
    companies.each do |production_company_raw_uri|
      production_company_mapped_uri = "#{BASE_NAMESPACE}/company/"
      ids = @virtuoso_reader.get_objects_for(
          subject: production_company_raw_uri,
          predicate: "#{TMDB_NAMESPACE}movie/production_companies/id"
      )
      ids.each do |production_company_id|
        production_company_mapped_uri += "#{production_company_id}"
        @virtuoso_writer.new_triple(
            production_company_mapped_uri, "#{RDF_NAMESPACE}type", "#{SCHEMA_NAMESPACE}Organization", literal: false
        )
      end if ids
      names = @virtuoso_reader.get_objects_for(
          subject: production_company_raw_uri,
          predicate: "#{TMDB_NAMESPACE}movie/production_companies/name"
      )
      names.each do |production_company_name|
        @virtuoso_writer.new_triple(
            production_company_mapped_uri, "#{SCHEMA_NAMESPACE}name", production_company_name
        )
      end if names
      @virtuoso_writer.new_triple(
          raw_db_uri, "#{SCHEMA_NAMESPACE}productionCompany", production_company_mapped_uri, literal: false
      )
    end if companies
  end

  def map_cast(raw_db_uri)
    casts = @virtuoso_reader.get_objects_for(
        subject: raw_db_uri,
        predicate: "#{TMDB_NAMESPACE}movie/credits/cast"
    )
    casts.each do |cast_raw_uri|
      cast_mapped_uri = "#{BASE_NAMESPACE}/performance/"
      ids = @virtuoso_reader.get_objects_for(
          subject: cast_raw_uri,
          predicate: "#{TMDB_NAMESPACE}cast/id"
      )
      ids.each do |cast_id|
        cast_mapped_uri += "#{cast_id}"
        @virtuoso_writer.new_triple(
            cast_mapped_uri, "#{RDF_NAMESPACE}type", "#{LOM_NAMESPACE}Performance", literal: false
        )
      end if ids
      names = @virtuoso_reader.get_objects_for(
          subject: cast_raw_uri,
          predicate: "#{TMDB_NAMESPACE}cast/name"
      )
      names.each do |performance_name|
        @virtuoso_writer.new_triple(
            cast_mapped_uri, "#{LOM_NAMESPACE}actor", performance_name
        )
      end if names
      characters = @virtuoso_reader.get_objects_for(
          subject: cast_raw_uri,
          predicate: "#{TMDB_NAMESPACE}cast/character"
      )
      characters.each do |performance_character|
        @virtuoso_writer.new_triple(
            cast_mapped_uri, "#{LOM_NAMESPACE}character", performance_character
        )
      end if characters
      @virtuoso_writer.new_triple(
          cast_raw_uri, "#{LOM_NAMESPACE}performance", cast_mapped_uri, literal: false
      )
    end if casts
  end



  # def get_objects_for(movie_id)
  #   # TODO read triple from virtuoso where id==movie_id
  # end
  #
  # def type_has_mapping?(type)
  #   mapping_type = %W(
  #   #{tmdb}Movie #{tmdb}TV #{tmdb}TV_Season
  #                     #{tmdb}TV_Episode #{tmdb}Person
  #                  )
  #
  #   return true if mapping_type.include? predicate
  #   puts "Will not be mapped: type ``#{type}''"
  #   false
  # end
  #
  # def predicate_has_mapping?(predicate)
  #   mapping_predicate = %W(
  #   #{TMDB_NAMESPACE}movie/title #{TMDB_NAMESPACE}movie/release_date
  #     #{TMDB_NAMESPACE}movie/production_companies
  #     #{TMDB_NAMESPACE}cast/character #{TMDB_NAMESPACE}cast/name #{TMDB_NAMESPACE}crew/job
  #     #{TMDB_NAMESPACE}person/birthday #{TMDB_NAMESPACE}person/place_of_birth #{TMDB_NAMESPACE}person/name
  #   )
  #   #  #{TMDB_NAMESPACE}tv/season/air_date #{TMDB_NAMESPACE}tv/season/episode/air_date
  #   #  #{TMDB_NAMESPACE}tv/first_air_date #{TMDB_NAMESPACE}tv/last_air_date #{TMDB_NAMESPACE}tv/seasons
  #   #  #{TMDB_NAMESPACE}tv/number_of_seasons #{TMDB_NAMESPACE}tv/number_of_episodes
  #   #  #{TMDB_NAMESPACE}tv/season/season_number #{TMDB_NAMESPACE}tv/season/episodes
  #   #  #{TMDB_NAMESPACE}tv/season/episode/episode_number #{TMDB_NAMESPACE}tv/season/episode/season_number
  #
  #
  #   return true if mapping_predicate.include? predicate
  #   puts "Will not be mapped: predicate ``#{predicate}''"
  #   false
  # end
  #
  #
  # # TODO do not take triples separately but each entity as a whole
  # # TODO (Ctd) in that way, we know e.g. the ID of a cast
  # # TODO therefore I have to know how the RAW data of TMDb is exactly stored in Virtuoso
  # # TODO map/merge duplicated entities
  # # TODO find sameAs links to existing entities (e.g. Brad Pitt on dbpedia)
  # # TODO map Places or find existing places on dbpedia and define sameAs
  # # TODO add "@en" to literals?
  # # TODO is DateTime recognized as xsd:date? or how should we store the date in triples?
  # # TODO is storing an integer/decimal as literal enough? should we add another triple to define the type?
  # def _map(subject, predicate, object, id)
  #   case subject
  #
  #     when "#{tmdb}Movie"
  #       case predicate
  #         when "#{tmdb}movie/title" or "#{tmdb}tv/name"
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}Movie", "#{schema}name", object
  #         when "#{tmdb}movie/release_date" or "#{tmdb}tv/season/air_date" or "#{tmdb}tv/season/episode/air_date"
  #           date = object.split('-')
  #           puts "Other date format: {object}" if date.length != 3
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}Movie", "#{schema}datePublished", DateTime.new(date.first, date[1], date.last)
  #         when "#{tmdb}movie/production_companies"
  #           entity = "#{base}Movie/#{id}/productionCompany/#{object}"
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}Movie", "#{schema}productionCompany", entity
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{rdf}type", "#{schema}Organization"
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{schema}name", object
  #         when "#{tmdb}cast/character"
  #           # TODO ID of cast should be used for URIs
  #           entity = "#{base}Movie/#{id}/cast/#{object}"
  #           entity2 = "#{base}Movie/#{id}/cast/#{object}/character"
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}Movie", "#{lom}performance", entity
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{rdf}type", "#{lom}Performance"
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{lom}character", entity2
  #           TMDbMapper::VirtuosoWriter.new_triple entity2, "#{rdf}type", "#{dbpedia}FictionalCharacter"
  #           TMDbMapper::VirtuosoWriter.new_triple entity2, "#{schema}name", object
  #         when "#{tmdb}cast/name"
  #           # TODO ID of cast should be used for URIs (so that character and actor reference the the same performance)
  #           entity = "#{base}Movie/#{id}/cast/#{object}"
  #           entity2 = "#{base}Movie/#{id}/cast/#{object}/actor"
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}Movie", "#{lom}performance", entity
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{rdf}type", "#{lom}Performance"
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{lom}actor", entity2
  #           TMDbMapper::VirtuosoWriter.new_triple entity2, "#{rdf}type", "#{dbpedia}Actor"
  #           TMDbMapper::VirtuosoWriter.new_triple entity2, "#{schema}name", object
  #           name = object.split(' ')
  #             # TODO use all names (currently only first token is used as given and last token is used as family name)
  #           TMDbMapper::VirtuosoWriter.new_triple entity2, "#{schema}givenName", name.first
  #           TMDbMapper::VirtuosoWriter.new_triple entity2, "#{schema}familyName", name.last
  #         when "#{tmdb}crew/job" and object.to_s.include? "Director"
  #           entity = "#{base}Movie/#{id}/director/#{object}"
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}Movie", "#{schema}director", entity
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{rdf}type", "#{lom}Director"
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{schema}name", object
  #           name = object.split(' ')
  #           # TODO use all names (currently only first token is used as given and last token is used as family name)
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{schema}givenName", name.first
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{schema}familyName", name.last
  #       end
  #
  #     when "#{tmdb}TV"
  #       case predicate
  #         when "#{tmdb}tv/first_air_date"
  #           date = object.split('-')
  #           puts "Other date format: {object}" if date.length != 3
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}TVSeries", "#{schema}startDate", DateTime.new(date.first, date[1], date.last)
  #         when "#{tmdb}tv/last_air_date"
  #           date = object.split('-')
  #           puts "Other date format: {object}" if date.length != 3
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}TVSeries", "#{schema}endDate", DateTime.new(date.first, date[1], date.last)
  #         when "#{tmdb}tv/seasons"
  #           # TODO what contains 'object' in case of season? does it make sense to name the entity like that?
  #           # TODO would be good to use ID for URI
  #           entity = "#{base}TVSeries/#{id}/season/#{object}"
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}TVSeries", "#{schema}season", entity
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{rdf}type", "#{schema}TVSeason"
  #           # TODO maybe an ID or name of season should be stored in triple as well to map identical seasons later once
  #           # TODO (Ctd.) but how can we get the ID or name?
  #         when "#{tmdb}tv/number_of_seasons"
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}TVSeries", "#{schema}numberOfSeasons", object
  #         when "#{tmdb}tv/number_of_episodes"
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}TVSeries", "#{schema}numberOfEpisodes", object
  #       end
  #
  #     when "#{tmdb}TV_Season"
  #       case predicate
  #         when "#{tmdb}tv/season/season_number"
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}TVSeason", "#{schema}seasonNumber", object
  #         when "#{tmdb}tv/season/episodes"
  #           # TODO what contains 'object' in case of episode? does it make sense to name the entity like that?
  #           # TODO would be good to use ID for URI
  #           entity = "#{base}TVSeries/#{id}/season/episode/#{object}"
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}TVSeason", "#{schema}episode", entity
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{rdf}type", "#{schema}Episode"
  #           # TODO maybe an ID or name of episode should be stored in triple as well to map identical seasons later once
  #           # TODO (Ctd.) but how can we get the ID or name?
  #       end
  #
  #     when "#{tmdb}TV_Episode"
  #       case predicate
  #         when "#{tmdb}tv/season/episode/episode_number"
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}Episode", "#{schema}episodeNumber", object
  #         when "#{tmdb}tv/season/episode/season_number"
  #           entity = "#{base}TVSeries/#{id}/season/#{object}"
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}Episode", "#{schema}partOfSeason", entity
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{rdf}type", "#{schema}TVSeason"
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{schema}seasonNumber", object
  #       end
  #
  #     when "#{tmdb}Person"
  #       case predicate
  #         when "#{tmdb}person/birthday"
  #           date = object.split('-')
  #           puts "Other date format: {object}" if date.length != 3
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}Person", "#{schema}birthDate", DateTime.new(date.first, date[1], date.last)
  #         when "#{tmdb}person/place_of_birth"
  #           entity = "#{base}Person/#{id}/birthPlace/#{object}"
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}Person", "#{dbpedia}birthPlace", entity
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{rdf}type", "#{dbpedia}Place"
  #           TMDbMapper::VirtuosoWriter.new_triple entity, "#{rdfs}label", object
  #         when "#{tmdb}person/name"
  #           name = object.split(' ')
  #           # TODO use all names (currently only first token is used as given and last token is used as family name)
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}Person", "#{schema}givenName", name.first
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}Person", "#{schema}familyName", name.last
  #           # complete name is mapped to 'name'
  #           TMDbMapper::VirtuosoWriter.new_triple "#{schema}Person", "#{schema}name", object
  #       end
  #   end
  # end

  private
  def secrets
    @secrets ||= YAML.load_file '/config/secrets.yml'
  end
end
