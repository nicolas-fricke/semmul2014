require 'yaml'
require 'time'
require 'logger'

class DBpediaMapper::Mapper

  def initialize    
    load_schemas()
    load_graphs()

    @log = Logger.new('log', 'daily')

    @virtuoso_writer = VirtuosoWriter.new
    @virtuoso_writer.set_graph('mapped')
    @virtuoso_reader = VirtuosoReader.new
    @virtuoso_reader.set_graph 'raw'

    @publisher = MsgPublisher.new
    @publisher.set_queue('mapping')

    # encode (because sparql cannot handle these characters)
    @fallback = {
      'Š'=>'S', 'š'=>'s', 'Ð'=>'Dj','Ž'=>'Z', 'ž'=>'z', 'À'=>'A', 'Á'=>'A', 'Â'=>'A', 'Ã'=>'A', 'Ä'=>'A',
      'Å'=>'A', 'Æ'=>'A', 'Ç'=>'C', 'È'=>'E', 'É'=>'E', 'Ê'=>'E', 'Ë'=>'E', 'Ì'=>'I', 'Í'=>'I', 'Î'=>'I',
      'Ï'=>'I', 'Ñ'=>'N', 'Ò'=>'O', 'Ó'=>'O', 'Ô'=>'O', 'Õ'=>'O', 'Ö'=>'O', 'Ø'=>'O', 'Ù'=>'U', 'Ú'=>'U',
      'Û'=>'U', 'Ü'=>'U', 'Ý'=>'Y', 'Þ'=>'B', 'ß'=>'Ss','à'=>'a', 'á'=>'a', 'â'=>'a', 'ã'=>'a', 'ä'=>'a',
      'å'=>'a', 'æ'=>'a', 'ç'=>'c', 'è'=>'e', 'é'=>'e', 'ê'=>'e', 'ë'=>'e', 'ì'=>'i', 'í'=>'i', 'î'=>'i',
      'ï'=>'i', 'ð'=>'o', 'ñ'=>'n', 'ò'=>'o', 'ō'=>'o', 'ó'=>'o', 'ô'=>'o', 'õ'=>'o', 'ö'=>'o', 'ø'=>'o',
      'ù'=>'u', 'ú'=>'u', 'û'=>'u', 'ý'=>'y', 'ý'=>'y', 'þ'=>'b', 'ÿ'=>'y', 'ƒ'=>'f'
    }

    @date_formatting = [get_property('schema', 'datePublished'), get_property('schema', 'birthDate')]
    
    @further_entities = [get_property('schema', 'director'), get_property('schema', 'productionCompany'), get_property('dbpedia', 'birthPlace'), get_property('dbpedia', 'actor')]
    
    @literals = [get_property('schema', 'name'), get_property('lom', 'imdb_id'), get_property('lom', 'freebase_mid'), get_property('schema', 'datePublished'), get_property('schema', 'givenName'), get_property('schema', 'familyName'), get_property('schema', 'birthDate'), get_property('schema', 'alternateName')]

    @runtimes_minutes = [get_property('dbpedia', 'Work/runtime'), get_property('lom', 'runtime')]
    @runtimes_seconds = [get_property('dbprop', 'runtime/60'), get_property('dbpedia', 'runtime/60'), get_property('dbpedia', 'runtime')]

    @object_mappings = {
      get_property('owl', 'Thing') => get_property('schema', 'Thing'),
      get_property('dbpedia', 'Work') => get_property('schema', 'CreativeWork'),
      get_property('dbpedia', 'Film') => get_property('schema', 'Movie'),
      # get_property('dbpedia', 'TelevisionShow') => get_property('schema', 'TVSeries'),
      # get_property('dbpedia', 'TelevisionSeason') => get_property('schema', 'TVSeason'),
      # get_property('dbpedia', 'TelevisionEpisode') => get_property('schema', 'Episode'),
      get_property('foaf', 'Person') => get_property('schema', 'Person'),
      get_property('dbpedia', 'Person') => get_property('schema', 'Person'),

      get_property('dbpedia', 'Organisation') => get_property('schema', 'Organization'),
      get_property('dbpedia', 'Company') => get_property('schema', 'Organization'),

      get_property('dbpedia', 'Actor') => get_property('dbpedia', 'Actor'),
      get_property('dbpedia', 'FictionalCharacter') => get_property('dbpedia', 'FictionalCharacter'),
      
      get_property('schema', 'Thing') => get_property('schema', 'Thing'),
      get_property('schema', 'CreativeWork') => get_property('schema', 'CreativeWork'),
      get_property('schema', 'Movie') => get_property('schema', 'Movie'),
      # get_property('schema', 'TVSeries') => get_property('schema', 'TVSeries'),
      # get_property('schema', 'TVSeason') => get_property('schema', 'TVSeason'),
      # get_property('schema', 'Episode') => get_property('schema', 'Episode'),
      get_property('schema', 'Person') => get_property('schema', 'Person'),

      get_property('schema', 'Organization') => get_property('schema', 'Organization'),
      get_property('schema', 'Organisation') => get_property('schema', 'Organization'),
      get_property('schemardfs', 'Organisation') => get_property('schema', 'Organization'),
    }

    # not tested:
    # episodeList
    # partOfSeries
    # episodeNumber
    # partOfSeason

    # name: "first last" and "last, first"
    # sameAs: direction
    @property_mappings = {
      get_property('dbprop', 'name') => get_property('schema', 'name'), # literal, string
      get_property('foaf', 'name') => get_property('schema', 'name'),
      get_property('schema', 'name') => get_property('schema', 'name'),

      get_property('owl', 'sameAs') => get_property('schema', 'sameAs'), # URI
      get_property('schema', 'sameAs') => get_property('schema', 'sameAs'),

      get_property('dbpedia', 'imdbId') => get_property('lom', 'imdb_id'), # literal, string
      get_property('lom', 'imdb_id') => get_property('lom', 'imdb_id'),

      get_property('lom', 'freebase_mid') => get_property('lom', 'freebase_mid'), # literal, string

      get_property('dbpedia', 'releaseDate') => get_property('schema', 'datePublished'), # literal, date
      get_property('dbprop', 'releaseDate') => get_property('schema', 'datePublished'),
      get_property('schema', 'datePublished') => get_property('schema', 'datePublished'),

      get_property('dbpedia', 'director') => get_property('schema', 'director'), # URI
      get_property('dbprop', 'director') => get_property('schema', 'director'),
      get_property('schema', 'director') => get_property('schema', 'director'),

      get_property('dbpedia', 'distributor') => get_property('schema', 'productionCompany'), # URI
      get_property('dbprop', 'distributor') => get_property('schema', 'productionCompany'),
      get_property('dbprop', 'distributors') => get_property('schema', 'productionCompany'),
      get_property('dbprop', 'studio') => get_property('schema', 'productionCompany'),
      get_property('schema', 'productionCompany') => get_property('schema', 'productionCompany'),

      # get_property('dbprop', 'firstAired') => get_property('schema', 'startDate'), # literal, date
      # get_property('schema', 'startDate') => get_property('schema', 'startDate'),

      # get_property('dbprop', 'lastAired') => get_property('schema', 'endDate'), #literal, date
      # get_property('schema', 'endDate') => get_property('schema', 'endDate'),

      # get_property('dbpedia', 'numberOfSeasons') => get_property('schema', 'numberOfSeasons'), # literal, integer
      # get_property('dbprop', 'numSeasons') => get_property('schema', 'numberOfSeasons'),
      # get_property('schema', 'numberOfSeasons') => get_property('schema', 'numberOfSeasons'),

      # get_property('dbpedia', 'numberOfEpisodes') => get_property('schema', 'numberOfEpisodes'), # literal, integer
      # get_property('dbprop', 'numEpisodes') => get_property('schema', 'numberOfEpisodes'),
      # get_property('schema', 'numberOfEpisodes') => get_property('schema', 'numberOfEpisodes'),

      # get_property('dbprop', 'episodeList') => get_property('schema', 'episode'), # URI
      # get_property('schema', 'episode') => get_property('schema', 'episode'),

      # get_property('dbprop', 'series') => get_property('schema', 'partOfSeries'), # URI
      # get_property('schema', 'partOfSeries') => get_property('schema', 'partOfSeries'),

      # get_property('dbpedia', 'episodeNumber') => get_property('schema', 'episodeNumber'), # literal, integer
      # get_property('dbprop', 'episode') => get_property('schema', 'episodeNumber'),
      # get_property('schema', 'episodeNumber') => get_property('schema', 'episodeNumber'),

      # get_property('dbpedia', 'seasonNumber') => get_property('schema', 'partOfSeason'), # URI
      # get_property('dbprop', 'season') => get_property('schema', 'partOfSeason'),
      # get_property('schema', 'partOfSeason') => get_property('schema', 'partOfSeason'),

      get_property('foaf', 'givenName') => get_property('schema', 'givenName'), # literal, string
      get_property('schema', 'givenName') => get_property('schema', 'givenName'),

      get_property('foaf', 'surname') => get_property('schema', 'familyName'), # literal, string
      get_property('schema', 'familyName') => get_property('schema', 'familyName'),

      get_property('dbpedia', 'birthDate') => get_property('schema', 'birthDate'), # literal, date
      get_property('dbprop', 'birthDate') => get_property('schema', 'birthDate'),
      get_property('schema', 'birthDate') => get_property('schema', 'birthDate'),

      get_property('dbpedia', 'birthPlace') => get_property('dbpedia', 'birthPlace'), # URI
      get_property('dbprop', 'placeOfBirth') => get_property('dbpedia', 'birthPlace'),

      get_property('dbpedia', 'starring') => get_property('lom', 'performance'), # URI
      get_property('dbprop', 'starring') => get_property('lom', 'performance'),
      get_property('lom', 'performance') => get_property('lom', 'performance'),

      get_property('dbprop', 'alternativeNames') => get_property('schema', 'alternateName'), # literal, string
      get_property('dbpedia', 'alias') => get_property('schema', 'alternateName'),
      get_property('schema', 'alternateName') => get_property('schema', 'alternateName'),

      get_property('dbpedia', 'abstract') => get_property('schema', 'description'), # literal, string
      get_property('schema', 'description') => get_property('schema', 'description'),

      get_property('dbpedia', 'Work/runtime') => get_property('lom', 'runtime'), # literal, double (in minutes)
      get_property('dbprop', 'runtime/60') => get_property('lom', 'runtime'),
      get_property('dbpedia', 'runtime/60') => get_property('lom', 'runtime'),
      get_property('dbpedia', 'runtime') => get_property('lom', 'runtime'),
      get_property('lom', 'runtime') => get_property('lom', 'runtime'),
    }

    @type = get_property('rdf', 'type')
  end

  def register_receiver
    @receiver = MsgConsumer.new
    @receiver.set_queue 'raw_dbpedia'
    @receiver.subscribe(type: "movie") { |movie_uri| map_entity(movie_uri, true) }
    # map_entity("http://dbpedia.org/resource/Star_Trek_(film)", true)
  end

  def start_demo(demoset = [])
    p "starting dbpedia mapper in demo mode"
    i = 1
    demoset.each do |movie_uri|
      puts "\nMapping Movie #{i} / #{demoset.size}\n"
      map_entity movie_uri, true
      i += 1
    end
    p "dbpedia mapper done"
  end

  def mapped_object(object)
    @object_mappings["#{object}"].to_s
  end

  def mapped_property(property)
    @property_mappings["#{property}"].to_s
  end


  def name(uri)
    uri.to_s[(uri.to_s.rindex('/') + 1)..-1]
  end


  def map(subject, predicate, object, go_deeper)
    if predicate == @type
      mo = mapped_object(object)
      if mo != nil and mo != ""
        @virtuoso_writer.new_triple(subject, @type, mo, literal:false)
      end
    else
      mp = mapped_property(predicate)
      
      # map dates
      if @date_formatting.include?(mp)
        begin
          if mp == get_property('schema', 'datePublished')
            property_date_year = get_property('schema', 'yearPublished')
          elsif mp == get_property('schema', 'birthDate')
            property_date_year = get_property('schema', 'birthYear')
          end
            
          # if date is complete
          if object.to_s=~/^(?<year>(18|19|20)\d{2})-(?<month>(0[1-9]|1[012]))\-(?<day>(0[1-9]|[12][0-9]|3[01]))$/
            date_string = RDF::Literal.new(object.to_s, datatype: RDF::XSD.date)
            @virtuoso_writer.new_triple(subject, mp, date_string)
            date_string = RDF::Literal.new(object.to_s[0...4], datatype: RDF::XSD.gYear)
            @virtuoso_writer.new_triple(subject, property_date_year, date_string)
          # if only year (and month) is given
          elsif object.to_s=~/^(?<year>(18|19|20)\d{2})/
            date_string = RDF::Literal.new(object.to_s[0...4], datatype: RDF::XSD.gYear)
            @virtuoso_writer.new_triple(subject, property_date_year, date_string)
          end
        rescue ArgumentError
          @log.error "Could not parse release date `#{object.to_s}' as date."
        end

      # map actors in performances
      elsif mp == get_property('lom', 'performance')
        begin
          movie = name(subject)
          actor = name(object)
          performance = get_property('lom_dbpedia', "movie/#{movie}/performance/#{actor}")
          @virtuoso_writer.new_triple(subject, get_property('lom', 'performance'), performance, literal:false)
          @virtuoso_writer.new_triple(performance, get_property('rdf', 'type'), get_property('lom', 'Performance'), literal:false)
          @virtuoso_writer.new_triple(performance, get_property('lom', 'actor'), object, literal:false)
          @virtuoso_writer.new_triple(performance, @schemas['pav_lastupdateon'], RDF::Literal.new(DateTime.now, datatype: RDF::XSD.dateTime))

          map_entity(object, false)
          @virtuoso_writer.new_triple(object, get_property('rdf', 'typeOf'), get_property('dbpedia', 'Actor '), literal:false)
        rescue
        end

      # map objects with URIs to other entities that have to be mapped
      elsif go_deeper and @further_entities.include?(mp)
        map_entity(object, false)
        if mp == get_property('schema', 'director')
          @virtuoso_writer.new_triple(object, get_property('rdf', 'typeOf'), get_property('lom', 'Director'), literal:false)
        end
        @virtuoso_writer.new_triple(subject, mp, clean(object), literal:@literals.include?(mp))

      # map freebase ids
      elsif mp == get_property('schema', 'sameAs') and object.start_with?('http://rdf.freebase.com/ns/')
        mp = get_property('lom', 'freebase_mid')
        object = object.to_s
        mo = '/m/' + object[29, 20]
        @virtuoso_writer.new_triple(subject, mp, mo)

      # map minutes runtimes
      elsif @runtimes_minutes.include?(predicate)
        @virtuoso_writer.new_triple(subject, mp, RDF::Literal.new(object.to_s.to_i, datatype: RDF::XSD.integer))

      # map seconds runtimes
      elsif @runtimes_seconds.include?(predicate)
        @virtuoso_writer.new_triple(subject, mp, RDF::Literal.new(object.to_s.to_i / 60, datatype: RDF::XSD.integer))

      # map everything else
      elsif mp != nil and mp != ""
        @virtuoso_writer.new_triple(subject, mp, clean(object), literal:@literals.include?(mp))
      end
    end

  end


# TODO: sameAs links may have the current entity as object instead of subject - then they will not be mapped right now
  def map_entity(uri, initial)
    uri = clean(uri)
    p "mapping #{uri}"

    # delete existing triples
    @virtuoso_writer.delete_triple(subject: uri)

    values = @virtuoso_reader.get_values_for(subject: uri)
    if values != true and not values.nil?
      values.each_solution do |v|
        p = v.bindings[:p]
        o = v.bindings[:o]
        map(uri, p, o, initial)
      end
      @virtuoso_writer.new_triple(uri, @schemas['pav_lastupdateon'], RDF::Literal.new(DateTime.now, datatype: RDF::XSD.dateTime))
      if initial
        @publisher.enqueue :movie_uri, uri
      end
    end
  end

  def clean(string)
    begin
      string = string.to_s.encode('us-ascii', :fallback => @fallback)
      return string
    rescue Exception => e
      @log.error "Cannot convert '#{string}' to US-ASCII. Probably the string contains characters not included in fallback."
    end

    return ""
  end

  def get_property(schema, property)
    s = @schemas[schema]
    "#{s}#{property}"
  end

  def set_xsd_type(literal, type)
    "#{literal}^^#{@schemas['xsd']}#{type}"
  end

  private
  def secrets
    @secrets ||= YAML.load_file '../config/secrets.yml'
  end

  private
  def load_schemas
    file ||= YAML.load_file '../config/namespaces.yml'
    @schemas = file['schemas']
  end

  private
  def load_graphs
    file ||= YAML.load_file '../config/namespaces.yml'
    @graphs = file['graphs']
  end
end
