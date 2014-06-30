require 'yaml'
require 'time'

class DBpediaMapper::Mapper
  Base = "http://example.com/"
  Lom = "http://www.hpi.uni-potsdam.de/semmul2014/lodofmovies.owl#"
  Schema = "http://schema.org/"
  Dbpedia = "http://dbpedia.org/ontology/"
  Dbprop = "http://dbpedia.org/property/"
  Rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  Rdfs = "http://www.w3.org/2000/01/rdf-schema#"
  Owl = "http://www.w3.org/2002/07/owl#"
  # Foaf = "http://xmlns.com/foaf/spec/"
  Foaf = "http://xmlns.com/foaf/0.1/"

  Xsd = "http://www.w3.org/2001/XMLSchema#"

  Type = "#{Rdf}type"

  Object_mappings = {
    "#{Owl}Thing" => "#{Schema}Thing",
    "#{Dbpedia}Work" => "#{Schema}CreativeWork",
    "#{Dbpedia}Film" => "#{Schema}Movie",
    "#{Dbpedia}TelevisionShow" => "#{Schema}TVSeries",
    "#{Dbpedia}TelevisionSeason" => "#{Schema}TVSeason",
    "#{Dbpedia}TelevisionEpisode" => "#{Schema}Episode",
    "#{Foaf}Person" => "#{Schema}Person",
    "#{Dbpedia}Person" => "#{Schema}Person",

    "#{Dbpedia}Organisation" => "#{Schema}Organization",
    "#{Dbpedia}Company" => "#{Schema}Organization",
    
    "#{Dbpedia}Actor" => "#{Dbpedia}Actor",
    "#{Dbpedia}FictionalCharacter" => "#{Dbpedia}FictionalCharacter",
    
    "#{Schema}Thing" => "#{Schema}Thing",
    "#{Schema}CreativeWork" => "#{Schema}CreativeWork",
    "#{Schema}Movie" => "#{Schema}Movie",
    "#{Schema}TVSeries" => "#{Schema}TVSeries",
    "#{Schema}TVSeason" => "#{Schema}TVSeason",
    "#{Schema}Episode" => "#{Schema}Episode",
    "#{Schema}Person" => "#{Schema}Person",

    "#{Schema}Organization" => "#{Schema}Organization"
  }

  # not tested:
  # episodeList
  # partOfSeries
  # episodeNumber
  # partOfSeason

  # name: "first last" and "last, first"
  # sameAs: direction
  Property_mappings = {
    "#{Dbprop}name" => "#{Schema}name",
    "#{Foaf}name" => "#{Schema}name",
    "#{Schema}name" => "#{Schema}name",

    "#{Owl}sameAs" => "#{Schema}sameAs",
    "#{Schema}sameAs" => "#{Schema}sameAs",

    "#{Dbpedia}releaseDate" => "#{Schema}datePublished",
    "#{Dbprop}releaseDate" => "#{Schema}datePublished",
    "#{Schema}datePublished" => "#{Schema}datePublished",

    "#{Dbpedia}director" => "#{Schema}director",
    "#{Dbprop}director" => "#{Schema}director",
    "#{Schema}director" => "#{Schema}director",

    "#{Dbpedia}distributor" => "#{Schema}productionCompany",
    "#{Dbprop}distributor" => "#{Schema}productionCompany",
    "#{Dbprop}distributors" => "#{Schema}productionCompany",
    "#{Dbprop}studio" => "#{Schema}productionCompany",
    "#{Schema}productionCompany" => "#{Schema}productionCompany",

    "#{Dbprop}firstAired" => "#{Schema}startDate",
    "#{Schema}startDate" => "#{Schema}startDate",

    "#{Dbprop}lastAired" => "#{Schema}endDate",
    "#{Schema}endDate" => "#{Schema}endDate",

    "#{Dbpedia}numberOfSeasons" => "#{Schema}numberOfSeasons",
    "#{Dbprop}numSeasons" => "#{Schema}numberOfSeasons",
    "#{Schema}numberOfSeasons" => "#{Schema}numberOfSeasons",

    "#{Dbpedia}numberOfEpisodes" => "#{Schema}numberOfEpisodes",
    "#{Dbprop}numEpisodes" => "#{Schema}numberOfEpisodes",
    "#{Schema}numberOfEpisodes" => "#{Schema}numberOfEpisodes",

    "#{Dbprop}episodeList" => "#{Schema}episode",
    "#{Schema}episode" => "#{Schema}episode",

    "#{Dbpedia}series" => "#{Schema}partOfSeries",
    "#{Schema}partOfSeries" => "#{Schema}partOfSeries",

    "#{Dbpedia}episodeNumber" => "#{Schema}episodeNumber",
    "#{Dbprop}episode" => "#{Schema}episodeNumber",
    "#{Schema}episodeNumber" => "#{Schema}episodeNumber",

    "#{Dbpedia}seasonNumber" => "#{Schema}partOfSeason",
    "#{Dbprop}season" => "#{Schema}partOfSeason",
    "#{Schema}partOfSeason" => "#{Schema}partOfSeason",

    "#{Foaf}givenName" => "#{Schema}givenName",
    "#{Schema}givenName" => "#{Schema}givenName",

    "#{Foaf}surname" => "#{Schema}familyName",
    "#{Schema}familyName" => "#{Schema}familyName",

    "#{Dbpedia}birthDate" => "#{Schema}birthDate",
    "#{Dbprop}birthDate" => "#{Schema}birthDate",
    "#{Schema}birthDate" => "#{Schema}birthDate",

    "#{Dbpedia}birthPlace" => "#{Dbpedia}birthPlace",
    "#{Dbprop}placeOfBirth" => "#{Dbpedia}birthPlace",

    "#{Dbpedia}starring" => "#{Lom}actor",
    "#{Dbprop}starring" => "#{Lom}actor",
    "#{Lom}actor" => "#{Lom}actor",
  }

  Date_formatting = ["#{Schema}datePublished", "#{Schema}startDate", "#{Schema}endDate", "#{Schema}birthDate"]
  Further_entities = ["#{Schema}director", "#{Schema}productionCompany", "#{Schema}episode", "#{Schema}partOfSeries", "#{Schema}partOfSeason", "#{Lom}actor"]

  def initialize
    @virtuoso = DBpediaMapper::Virtuoso.new
  end


  def mapped_object(object)
    mo = Object_mappings["#{object}"]
    return "#{mo}"
  end

  def mapped_property(property)
    mp = Property_mappings["#{property}"]
    return "#{mp}"
  end


  def map(subject, predicate, object, go_deeper = false)
    if predicate == Type
      mo = mapped_object(object)
      if mo != nil and mo != ""
        @virtuoso.write_mapped(subject, Type, mo)
      end
    else
      mp = mapped_property(predicate)
      
      # map dates
      if Date_formatting.include?(mp)
        begin
          date_string = Date.parse(object.to_s).xmlschema
          @virtuoso.write_mapped(subject, mp, "#{date_string}^^#{Xsd}date")
        rescue ArgumentError
          puts "Could not parse `#{object.to_s}' as date."
        end

      # map objects with URIs to other entities that have to be mapped
      elsif go_deeper and Further_entities.include?(mp)
        map_entity(object)
        @virtuoso.write_mapped(subject, mp, object)

      # map everything else
      elsif mp != nil and mp != ""
        @virtuoso.write_mapped(subject, mp, object)
      end
    end

  end


# TODO: sameAs links may have the current entity as object - then they will not be mapped right now
  def map_entity(uri, go_deeper = false)
    values = @virtuoso.get_all_for_subject(uri)
    values.each_solution do |v|
      p = v.bindings[:p]
      o = v.bindings[:o]
      map(uri, p, o, go_deeper)
    end
  end


  private
  def secrets
    @secrets ||= YAML.load_file '../config/secrets.yml'
  end
end