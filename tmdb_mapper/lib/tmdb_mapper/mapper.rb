require 'time'
require 'yaml'
require 'logger'

class TMDbMapper::Mapper

  # TODO check all literals (especially dates and numbers) for logical correctness

  def initialize
    load_schemas()
    @log = Logger.new('log', 'daily')
    @publisher = MsgPublisher.new
    @publisher.set_queue 'mapping'
    @virtuoso_writer = VirtuosoWriter.new
    @virtuoso_writer.set_graph 'mapped'
    @virtuoso_reader = VirtuosoReader.new
    @virtuoso_reader.set_graph 'raw'
    @dbpedia_reader = TMDbMapper::DBpediaReader.new
  end

  def register_receiver
    @receiver = MsgConsumer.new
    @receiver.set_queue 'raw_tmdb'
    @receiver.subscribe(type: :movie_uri) { |movie_uri| map(movie_uri) }
  end

  def start_demo(demoset = [])
    p "start tmdb mapper in demo mode"
    demoset.each do |movie_uri|
      map movie_uri
    end
    p "tmdb mapper done"
  end

  def map(raw_db_uri)
    # try to delete existing triples for movie first
    p "mapping #{raw_db_uri}"
    @virtuoso_writer.delete_triple(
        subject: raw_db_uri)

    # add new triples
    @virtuoso_writer.new_triple raw_db_uri, "#{@schemas['rdf']}type", "#{@schemas['schema']}Movie", literal: false
    map_movie_id(raw_db_uri)
    map_movie_titles(raw_db_uri)
    map_movie_description(raw_db_uri)
    map_movie_genres(raw_db_uri)
    map_movie_runtime(raw_db_uri)
    map_movie_tagline(raw_db_uri)
    map_movie_release_dates(raw_db_uri)
    map_movie_production_companies(raw_db_uri)
    map_cast(raw_db_uri)
    map_director(raw_db_uri)
    @virtuoso_writer.new_triple raw_db_uri, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
    @publisher.enqueue :movie_uri, raw_db_uri
  end

  def map_movie_id(raw_db_uri)
    tmdb_ids = @virtuoso_reader.get_objects_for(
        subject: raw_db_uri,
        predicate: "#{@schemas['tmdb']}movie/id"
    )
    tmdb_ids.each do |tmdb_id|
      @virtuoso_writer.new_triple(
          raw_db_uri, "#{@schemas['lom']}tmdb_id", tmdb_id
      ) if tmdb_id.to_s.length > 1
    end if tmdb_ids
    imdb_ids = @virtuoso_reader.get_objects_for(
        subject: raw_db_uri,
        predicate: "#{@schemas['tmdb']}movie/imdb_id"
    )
    imdb_ids.each do |imdb_id|
      @virtuoso_writer.new_triple(
          raw_db_uri, "#{@schemas['lom']}imdb_id", imdb_id
      ) if imdb_id.to_s.length > 1
    end if imdb_ids
  end

  def map_movie_titles(raw_db_uri)
    titles = @virtuoso_reader.get_objects_for(
      subject: raw_db_uri,
      predicate: "#{@schemas['tmdb']}movie/title"
    )
    titles.each do |title|
      @virtuoso_writer.new_triple(
          raw_db_uri, "#{@schemas['schema']}name", title
      ) if title.to_s.length > 1
      # puts title.to_s
    end if titles
  end

  def map_movie_description(raw_db_uri)
    description = @virtuoso_reader.get_objects_for(
        subject: raw_db_uri,
        predicate: "#{@schemas['tmdb']}movie/overview"
    )
    if description
      description.each do |d|
        @virtuoso_writer.new_triple(
            raw_db_uri, "#{@schemas['schema']}description", d
        ) if d.to_s.length > 1
      end if description
    end
  end

  def map_movie_genres(raw_db_uri)
    genre_uris = @virtuoso_reader.get_objects_for(
        subject: raw_db_uri,
        predicate: "#{@schemas['tmdb']}movie/genres"
    )
    if genre_uris
      genre_uris.each do |uri|
        genres = @virtuoso_reader.get_objects_for(
            subject: uri,
            predicate: "#{@schemas['tmdb']}genres/name"
        )
        genres.each do |genre|
          @virtuoso_writer.new_triple(
              raw_db_uri, "#{@schemas['schema']}genre", genre
          ) if genre.to_s.length > 1
        end if genres
      end if genre_uris
    end
  end

  def map_movie_runtime(raw_db_uri)
    runtime = @virtuoso_reader.get_objects_for(
        subject: raw_db_uri,
        predicate: "#{@schemas['tmdb']}movie/runtime"
    )
    if runtime
      runtime.each do |time|
        @virtuoso_writer.new_triple(
            raw_db_uri, "#{@schemas['lom']}runtime", time
        ) if time.to_s.length > 1
      end if runtime
    end
  end

  def map_movie_tagline(raw_db_uri)
    taglines = @virtuoso_reader.get_objects_for(
        subject: raw_db_uri,
        predicate: "#{@schemas['tmdb']}movie/tagline"
    )
    if taglines
      taglines.each do |tagline|
        @virtuoso_writer.new_triple(
            raw_db_uri, "#{@schemas['schema']}headline", tagline
        ) if tagline.to_s.length > 1
      end if taglines
    end
  end

  def map_movie_release_dates(raw_db_uri)
    dates = @virtuoso_reader.get_objects_for(
      subject: raw_db_uri,
      predicate: "#{@schemas['tmdb']}movie/release_date"
    )
    if dates
      dates.each do |release_date|
        # if date is complete
        if release_date.to_s=~/^(?<year>(19|20)\d{2})-(?<month>(0[1-9]|1[012]))\-(?<day>(0[1-9]|[12][0-9]|3[01]))$/
          @virtuoso_writer.new_triple(
              raw_db_uri, "#{@schemas['schema']}datePublished", (set_xsd_type release_date, 'date')
          )
          @virtuoso_writer.new_triple(
              raw_db_uri, "#{@schemas['lom']}yearPublished", (set_xsd_type release_date.to_s[0...4], 'gYear')
          )
        # if only year (and month) is given
        elsif release_date.to_s=~/^(?<year>(19|20)\d{2})/
          @virtuoso_writer.new_triple(
              raw_db_uri, "#{@schemas['lom']}yearPublished", (set_xsd_type release_date.to_s[0...4], 'gYear')
          )
        end
      end if dates
    end

    release_countries = @virtuoso_reader.get_objects_for(
        subject: raw_db_uri,
        predicate: "#{@schemas['tmdb']}movie/releases/countries"
    )
    if release_countries
      release_countries.each do |country_uri|
        isos = @virtuoso_reader.get_objects_for(
            subject: country_uri,
            predicate: "#{@schemas['tmdb']}movie/releases/countries/iso_3166_1"
        )
        # should be only one entry
        @virtuoso_writer.new_triple(
            raw_db_uri, "#{@schemas['lom']}release", country_uri, literal:false
        )
        @virtuoso_writer.new_triple(
            country_uri, "#{@schemas['rdf']}type", "#{@schemas['lom']}Release", literal:false
        )
        isos.each do |iso|
          @virtuoso_writer.new_triple(
              country_uri, "#{@schemas['lom']}country", iso
          )
        end if isos
        dates = @virtuoso_reader.get_objects_for(
            subject: country_uri,
            predicate: "#{@schemas['tmdb']}movie/releases/countries/release_date"
        )
        if dates
          dates.each do |date|
            # if date is complete
            if date.to_s=~/^(?<year>(19|20)\d{2})-(?<month>(0[1-9]|1[012]))\-(?<day>(0[1-9]|[12][0-9]|3[01]))$/
              @virtuoso_writer.new_triple(
                  country_uri, "#{@schemas['lom']}date", (set_xsd_type date, 'date')
              )
            end
          end if dates
        end
        certs = @virtuoso_reader.get_objects_for(
            subject: country_uri,
            predicate: "#{@schemas['tmdb']}movie/releases/countries/certification"
        )
        if certs
          certs.each do |cert|
            @virtuoso_writer.new_triple(
                country_uri, "#{@schemas['lom']}certification", cert
            ) if cert.to_s.length > 1
          end if certs
        end
        @virtuoso_writer.new_triple country_uri, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      end if release_countries
    end
  end

  def map_movie_production_companies(raw_db_uri)
    companies = @virtuoso_reader.get_objects_for(
        subject: raw_db_uri,
        predicate: "#{@schemas['tmdb']}movie/production_companies"
    )
    companies.each do |production_company_raw_uri|
      production_company_mapped_uri = "#{@schemas['base_tmdb']}company/"
      ids = @virtuoso_reader.get_objects_for(
          subject: production_company_raw_uri,
          predicate: "#{@schemas['tmdb']}movie/production_companies/id"
      )
      ids.each do |production_company_id|
        production_company_mapped_uri += "#{production_company_id}"
        # try to delete existing triples for company first
        @virtuoso_writer.delete_triple(
            subject: production_company_mapped_uri)

        # add new triples
        @virtuoso_writer.new_triple(
            production_company_mapped_uri, "#{@schemas['rdf']}type", "#{@schemas['schema']}Organization", literal: false
        ) if production_company_id.to_s.length > 1
        @virtuoso_writer.new_triple(
            production_company_mapped_uri, "#{@schemas['lom']}tmdb_id", production_company_id
        ) if production_company_id.to_s.length > 1
      end if ids
      names = @virtuoso_reader.get_objects_for(
          subject: production_company_raw_uri,
          predicate: "#{@schemas['tmdb']}movie/production_companies/name"
      )
      names.each do |production_company_name|
        @virtuoso_writer.new_triple(
            production_company_mapped_uri, "#{@schemas['schema']}name", production_company_name
        ) if production_company_name.to_s.length > 1
      end if names
      @virtuoso_writer.new_triple production_company_mapped_uri, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      @virtuoso_writer.new_triple(
          raw_db_uri, "#{@schemas['schema']}productionCompany", production_company_mapped_uri, literal: false
      )
    end if companies
  end

  def map_cast(raw_db_uri)
    casts = @virtuoso_reader.get_objects_for(
        subject: raw_db_uri,
        predicate: "#{@schemas['tmdb']}movie/credits/cast"
    )
    casts.each do |cast_raw_uri|
      cast_mapped_uri = "#{@schemas['base_tmdb']}performance/"
      ids = @virtuoso_reader.get_objects_for(
          subject: cast_raw_uri,
          predicate: "#{@schemas['tmdb']}cast/id"
      )
      ids.each do |cast_id|
        cast_mapped_uri += "#{cast_id}"
        # try to delete existing triples for cast first
        @virtuoso_writer.delete_triple(
            subject: cast_mapped_uri)

        # add new triples
        @virtuoso_writer.new_triple(
            cast_mapped_uri, "#{@schemas['rdf']}type", "#{@schemas['lom']}Performance", literal: false
        ) if cast_id.to_s.length > 1
        @virtuoso_writer.new_triple(
            cast_mapped_uri, "#{@schemas['lom']}tmdb_id", cast_id
        ) if cast_id.to_s.length > 1
      end if ids
      persons = @virtuoso_reader.get_objects_for(
          subject: cast_raw_uri,
          predicate: "#{@schemas['tmdb']}cast/person"
      )
      persons.each do |person_uri|
        @virtuoso_writer.new_triple(
            cast_mapped_uri, "#{@schemas['lom']}actor", person_uri
        ) if person_uri.to_s.length > 1
        # try to delete existing triples for person first
        @virtuoso_writer.delete_triple(
            subject: person_uri)

        # add new triples
        @virtuoso_writer.new_triple(
            person_uri, "#{@schemas['rdf']}type", "#{@schemas['dbpedia']}Actor", literal: false
        ) if person_uri.to_s.length > 1
        map_person person_uri
      end if persons
      characters = @virtuoso_reader.get_objects_for(
          subject: cast_raw_uri,
          predicate: "#{@schemas['tmdb']}cast/character"
      )
      characters.each do |performance_character|
        #character_uri = nil
        #character_uri = "#{cast_mapped_uri}/character" if performance_character.to_s.length > 1
        #if character_uri
          # try to delete existing triples for character first
          #@virtuoso_writer.delete_triple(
          #    subject: character_uri)

          # add new triples
        @virtuoso_writer.new_triple(
            cast_mapped_uri, "#{@schemas['lom']}character", performance_character
        )
          #@virtuoso_writer.new_triple(
          #    character_uri, "#{@schemas['rdf']}type", "#{@schemas['dbpedia']}FictionalCharacter", literal:false
          #)
          #@virtuoso_writer.new_triple(
          #    character_uri, "#{@schemas['rdf']}type", "#{@schemas['schema']}Person", literal:false
          #)
          #@virtuoso_writer.new_triple(
          #    character_uri, "#{@schemas['schema']}name", performance_character
          #)
          #@virtuoso_writer.new_triple character_uri, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')

        #end
      end if characters
      @virtuoso_writer.new_triple cast_mapped_uri, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
      @virtuoso_writer.new_triple(
          raw_db_uri, "#{@schemas['lom']}performance", cast_mapped_uri, literal: false
      )
    end if casts
  end

  def map_director(raw_db_uri)
    crews = @virtuoso_reader.get_objects_for(
        subject: raw_db_uri,
        predicate: "#{@schemas['tmdb']}movie/credits/crew"
    )
    crews.each do |crew_raw_uri|
      job = @virtuoso_reader.get_objects_for(
          subject: crew_raw_uri,
          predicate: "#{@schemas['tmdb']}crew/job"
      )
      # check whether job is 'director'
      if job.first.to_s.downcase == 'director'
        persons = @virtuoso_reader.get_objects_for(
            subject: crew_raw_uri,
            predicate: "#{@schemas['tmdb']}crew/person"
        )
        persons.each do |person_uri|
          @virtuoso_writer.new_triple(
              raw_db_uri, "#{@schemas['schema']}director", person_uri, literal: false
          )
          # try to delete existing triples for crew first
          @virtuoso_writer.delete_triple(
              subject: person_uri)

          # add new triples
          @virtuoso_writer.new_triple(
              person_uri, "#{@schemas['rdf']}type", "#{@schemas['lom']}Director", literal: false
          )
          map_person person_uri
        end if persons
      end
    end if crews
  end

  def map_person(person_uri)
    @virtuoso_writer.new_triple(
        person_uri, "#{@schemas['rdf']}type", "#{@schemas['schema']}Person", literal:false
    )
    ids = @virtuoso_reader.get_objects_for(
        subject: person_uri,
        predicate: "#{@schemas['tmdb']}person/id"
    )
    ids.each do |person_id|
      @virtuoso_writer.new_triple(
          person_uri, "#{@schemas['lom']}tmdb_id", person_id
      )
    end if ids
    names = @virtuoso_reader.get_objects_for(
        subject: person_uri,
        predicate: "#{@schemas['tmdb']}person/name"
    )
    names.each do |person_name|
      #person_names = person_name.to_s.split(' ')
      @virtuoso_writer.new_triple(
          person_uri, "#{@schemas['schema']}name", person_name
      )

      #@virtuoso_writer.new_triple(
      #    person_uri, "#{@schemas['schema']}givenName", person_names.first
      #)

      #@virtuoso_writer.new_triple(
      #    person_uri, "#{@schemas['schema']}familyName", person_names.last
      #)
    end if names
    aliases = @virtuoso_reader.get_objects_for(
        subject: person_uri,
        predicate: "#{@schemas['tmdb']}person/also_known_as"
    )
    aliases.each do |alias_name|
      @virtuoso_writer.new_triple(
          person_uri, "#{@schemas['schema']}alternateName", alias_name
      )
    end if aliases
    birthdates = @virtuoso_reader.get_objects_for(
        subject: person_uri,
        predicate: "#{@schemas['tmdb']}person/birthday"
    )
    birthdates.each do |date|
      # if date is complete
      if date.to_s=~/^(?<year>(19|20)\d{2})-(?<month>(0[1-9]|1[012]))\-(?<day>(0[1-9]|[12][0-9]|3[01]))$/
        @virtuoso_writer.new_triple(
            person_uri, "#{@schemas['schema']}birthDate", (set_xsd_type date, 'date')
        )
        @virtuoso_writer.new_triple(
            person_uri, "#{@schemas['dbpedia']}birthYear", (set_xsd_type date.to_s[0...4], 'gYear')
        )
        # if only year (and month) is given
      elsif date.to_s=~/^(?<year>(19|20)\d{2})/
        @virtuoso_writer.new_triple(
            person_uri, "#{@schemas['dbpedia']}birthYear", (set_xsd_type date.to_s[0...4], 'gYear')
        )
      end
    end if birthdates
    #birthplaces = @virtuoso_reader.get_objects_for(
    #    subject: person_uri,
    #    predicate: "#{@schemas['tmdb']}person/place_of_birth"
    #)
    #birthplaces.each do |place|
    #  place_uri = nil
    #  place_uri = @dbpedia_reader.get_place_uri place if place.to_s.length > 1
    #  if place_uri
    #    @virtuoso_writer.new_triple(
    #        person_uri, "#{@schemas['dbpedia']}birthPlace", place_uri, literal:false
    #    )
    #  end
    #end if birthplaces
    @virtuoso_writer.new_triple person_uri, @schemas['pav_lastupdateon'], (set_xsd_type DateTime.now, 'dateTime')
  end

  def set_xsd_type(literal, type)
    RDF::Literal.new(literal, datatype: "http://www.w3.org/2001/XMLSchema##{type}")
  end

  private
  def secrets
    @secrets ||= YAML.load_file '../config/secrets.yml'
  end

  private
  def load_schemas
    @schemas ||= YAML.load_file('../config/namespaces.yml')['schemas']
  end
end
