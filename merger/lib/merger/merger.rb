class Merger::Merger
  def initialize
    @matcher = Matcher::Matcher.new()
  end

  def register_receiver
    # receiver.subscribe(type: :movie_uri) { |movie_uri| merge(movie_uri) }
    merge 'http://rdf.freebase.com/ns/m/0hhqv27'
  end

  def merge(mapped_entity_uri)
    p "merging #{mapped_entity_uri}"
    # try to find entities with sameAs links
    main_db_entity_uri = find_merged_entity(mapped_entity_uri)
    if main_db_entity_uri
      merge_into_entity new_entity_uri: mapped_entity_uri, existing_entity_uri: main_db_entity_uri
    else
      # try to find entities that are identical
      main_db_entity_uri = find_matching_entity(mapped_entity_uri)
      if main_db_entity_uri
        # merge the entities
        merge_into_entity new_entity_uri: mapped_entity_uri, existing_entity_uri: main_db_entity_uri
      else
        # create a new entity in mainDB
        main_db_entity_uri = create_new_entity mapped_entity_uri: mapped_entity_uri
      end
      set_same_as_references main_db_uri: main_db_entity_uri, map_db_entry: mapped_entity_uri
    end
    update_provenience_information(main_db_entity_uri)
    publisher.enqueue :movie_uri, main_db_entity_uri
    main_db_entity_uri
  end

  def find_merged_entity(mapped_entity_uri)
    # (@Kerstin) Check if record in MainDB exists, that looks like { ?s sameAs mapped_entity_uri }
    # returns either merged_uri from MainDB entry or nil
    virtuoso_reader.set_graph 'merged'
    record =
        virtuoso_reader.get_subjects_for predicate: "#{schemas['owl']}sameAs",
                                         object: mapped_entity_uri
    p "find_merged_entity #{mapped_entity_uri} => #{record.nil? ? 'none' : record.first }"
    nil # TODO: Remove when output is removed
    record.first unless record.nil? # nil if no subject is found
  end

  def merge_into_entity(new_entity_uri:, existing_entity_uri:)
    "merge_into_entity(#{new_entity_uri}, #{existing_entity_uri})"
    # (@Kerstin) Per attribute from new record, merge into existing record
    virtuoso_reader.set_graph 'mapped'
    attributes_with_literals = virtuoso_reader.get_predicates_and_objects_for subject: new_entity_uri
    attributes_with_literals.each do |attribute|
      virtuoso_writer.new_triple existing_entity_uri, attribute[:p], attribute[:o]
    end
    attributes_with_uris = virtuoso_reader.get_predicates_and_objects_for subject: new_entity_uri
    attributes_with_uris.each do |attribute|
      merged_uri = if merge_predicate?(attribute[:p]) and attribute[:o].uri?
                     merge attribute[:o]
                   else
                     attribute[:o]
                   end

      virtuoso_writer.new_triple existing_entity_uri, attribute[:p], merged_uri, literal: false
    end
  end

  def find_matching_entity(mapped_entity_uri)
    # (@Flo) Employ matcher to find matching entity within MainDB
    # returns matching entity's URI or nil
    match = @matcher.find(mapped_entity_uri)
    p "find_matching_entity #{mapped_entity_uri} => #{match || 'none'}"
    match
  end

  def create_new_entity(mapped_entity_uri:)
    p "create_new_entity #{mapped_entity_uri}"
    # (@Nico) Copy entity from MapDB into MainDB and update URIs to match MainDB schema
    copy_machine = Merger::CopyMachine.new mapped_entity_uri, with_merger: self
    copy_machine.process # returns newly created entity URI
  end

  def set_same_as_references(main_db_uri:, map_db_entry:)
    # (@Nico)
    virtuoso_writer.new_triple main_db_uri,
                               "#{schemas['owl']}sameAs",
                               map_db_entry, literal: false
  end

  def update_provenience_information(main_db_entity_uri)
    # (@Kerstin)
    virtuoso_writer.delete_triple(
      predicate: schemas['pav_lastupdateon']
    )
    virtuoso_writer.new_triple(
      main_db_entity_uri, schemas['pav_lastupdateon'], RDF::Literal.new(DateTime.now, datatype: "#{schemas['xsd']}dateTime")
    )
  end

  private
  def merge_predicate?(predicate)
    !(predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
  end

  def publisher
    @publisher ||= MsgPublisher.new.tap {|p| p.set_queue 'merging' }
  end

  def receiver
    @receiver ||= MsgConsumer.new.tap {|r| r.set_queue 'mapping' }
  end

  def virtuoso_writer
    @virtuoso_writer ||= VirtuosoWriter.new(verbose: false).tap {|vw| vw.set_graph 'merged' }
  end

  def virtuoso_reader
    @virtuoso_reader ||= VirtuosoReader.new.tap {|vw| vw.set_graph 'mapped' }
  end

  def schemas
    @schemas ||= Merger::Config.namespaces['schemas']
  end
end