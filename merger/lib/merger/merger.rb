class Merger::Merger
  def initialize

  end

  def register_receiver
    # receiver.subscribe(type: :movie_uri) { |movie_uri| merge(movie_uri, is_movie: true) }

    # %w(
    # http://rdf.freebase.com/ns/m/0gtxbqr
    # http://rdf.freebase.com/ns/m/06zkfsy
    # http://rdf.freebase.com/ns/m/0lq6fb5
    # http://rdf.freebase.com/ns/m/05jzt3
    # http://rdf.freebase.com/ns/m/02ktj7
    # http://rdf.freebase.com/ns/m/02dr9j
    # http://semmul2014.hpi.de/tmdb/movie/152601
    # http://semmul2014.hpi.de/tmdb/movie/254
    # http://rdf.freebase.com/ns/m/08phg9
    # http://dbpedia.org/resource/Star_Trek_(film)
    # http://dbpedia.org/resource/Bambi
    # http://dbpedia.org/resource/Margin_Call_(film)
    # http://rdf.freebase.com/ns/m/0hhqv27
    # http://rdf.freebase.com/ns/m/04j1zjw
    # http://rdf.freebase.com/ns/m/07f_t4
    # http://rdf.freebase.com/ns/m/0dtfn
    # http://dbpedia.org/resource/Star_Trek_Into_Darkness
    # http://dbpedia.org/resource/300_(film)
    # http://dbpedia.org/resource/Star_Wars_Episode_IV:_A_New_Hope
    # http://dbpedia.org/resource/Fast_&_Furious_6
    # http://dbpedia.org/resource/Despicable_Me
    # http://dbpedia.org/resource/Her_(film)
    # http://dbpedia.org/resource/Walk_the_Line
    # http://dbpedia.org/resource/King_Kong_(1976_film)
    # http://dbpedia.org/resource/King_Kong_(2005_film)
    # http://semmul2014.hpi.de/tmdb/movie/13475
    # http://semmul2014.hpi.de/tmdb/movie/3170
    # http://semmul2014.hpi.de/tmdb/movie/1271
    # http://rdf.freebase.com/ns/m/0cc7hmk
    # http://semmul2014.hpi.de/tmdb/movie/54138
    # http://semmul2014.hpi.de/tmdb/movie/11
    # http://semmul2014.hpi.de/tmdb/movie/50839
    # http://semmul2014.hpi.de/tmdb/movie/69
    # http://semmul2014.hpi.de/tmdb/movie/82992
    # http://semmul2014.hpi.de/tmdb/movie/20352
    # http://semmul2014.hpi.de/tmdb/movie/10730
    # ).each do |movie_uri|


    # # two different king kong versions and star trek into darkness
    # %w(
    # http://dbpedia.org/resource/King_Kong_(1976_film)
    # http://rdf.freebase.com/ns/m/02ktj7
    # http://semmul2014.hpi.de/tmdb/movie/10730
    # http://dbpedia.org/resource/King_Kong_(2005_film)
    # http://rdf.freebase.com/ns/m/02dr9j
    # http://semmul2014.hpi.de/tmdb/movie/254
    # http://dbpedia.org/resource/Star_Trek_Into_Darkness
    # http://rdf.freebase.com/ns/m/0hhqv27
    # http://semmul2014.hpi.de/tmdb/movie/54138
    # ).each do |movie_uri|

    # star trek only
    %w(
    http://dbpedia.org/resource/Star_Trek_Into_Darkness
    http://rdf.freebase.com/ns/m/0hhqv27
    http://semmul2014.hpi.de/tmdb/movie/54138
    ).each do |movie_uri|

      merge movie_uri, is_movie: true
    end
  end

  def merge(mapped_uri, is_movie: false)
    p "merging #{mapped_uri}"
    # try to find entities with sameAs links
    main_db_uri = find_merged_entity mapped_uri
    if main_db_uri
      merge_into_entity new_uri: mapped_uri,
                        existing_uri: main_db_uri
    else
      # try to find entities that are identical
      if find_matching_entity mapped_uri
        # merge the entities
        merge_into_entity new_uri: mapped_uri,
                          existing_uri: main_db_uri
      else
        # create a new entity in mainDB
        main_db_uri = create_new_entity mapped_entity_uri: mapped_uri,
                                               is_movie: is_movie
      end
      set_same_as_references main_db_uri: main_db_uri,
                             map_db_entry: mapped_uri
    end
    update_provenience_information main_db_uri
    publisher.enqueue :movie_uri, main_db_uri
    main_db_uri
  end

  def find_merged_entity(mapped_uri)
    # Check if record in MainDB exists, that looks like { ?s sameAs mapped_entity_uri }
    # returns either merged_uri from MainDB entry or nil
    records = merged_reader.get_subjects_for predicate: "#{schemas['owl']}sameAs",
                                         object: mapped_uri

    records.first unless records.nil? # nil if no subject is found
  end

  def merge_into_entity(new_uri:, existing_uri:)
    # Per attribute from new record, merge into existing record
    mapped_reader.get_predicates_and_objects_for(subject: new_uri).each do |attribute|
      virtuoso_writer.new_triple existing_uri, 
                                 attribute[:p], 
                                 attribute[:o]
    end
    mapped_reader.get_predicates_and_objects_for(subject: new_uri).each do |attribute|
      merged_uri = if attribute[:o].uri? and should_be_merged?(attribute[:p])
                     merge attribute[:o]
                   else
                     attribute[:o]
                   end

      virtuoso_writer.new_triple existing_uri, attribute[:p], merged_uri, literal: false
    end
  end

  def find_matching_entity(mapped_uri)
    # Employ matcher to find matching entity within MainDB
    # returns matching entity's URI or nil
    matcher.find mapped_uri
  end

  def create_new_entity(mapped_entity_uri:, is_movie: false)
    # Copy entity from MapDB into MainDB and update URIs to match MainDB schema
    copy_machine = Merger::CopyMachine.new mapped_entity_uri,
                                           with_merger: self,
                                           type: (is_movie ? :movie : :generic)
    copy_machine.process # returns newly created entity URI
  end

  def set_same_as_references(main_db_uri:, map_db_entry:)
    virtuoso_writer.new_triple main_db_uri,
                               "#{schemas['owl']}sameAs",
                               map_db_entry, literal: false
  end

  def update_provenience_information(main_db_entity_uri)
    virtuoso_writer.delete_triple predicate: schemas['pav_lastupdateon']
    virtuoso_writer.new_triple main_db_entity_uri,
                               schemas['pav_lastupdateon'],
                               RDF::Literal.new(DateTime.now)
  end

  private
  def should_be_merged?(predicate)
    !(%w(
      http://www.w3.org/1999/02/22-rdf-syntax-ns#type
      http://schema.org/sameAs
    ).include? predicate)
  end

  def publisher
    @publisher ||= MsgPublisher.new.tap {|p| p.set_queue 'merging' }
  end

  def receiver
    @receiver ||= MsgConsumer.new.tap {|r| r.set_queue 'mapping' }
  end

  def virtuoso_writer
    @virtuoso_writer ||= VirtuosoWriter.new verbose: false, graph: 'merged'
  end

  def mapped_reader
    @mapped_reader ||= VirtuosoReader.new graph: 'mapped'
  end

  def merged_reader
    @merged_reader ||= VirtuosoReader.new graph: 'merged'
  end

  def schemas
    @schemas ||= Merger::Config.namespaces['schemas']
  end
  def matcher
    @matcher ||= Matcher::Matcher.new
  end
end