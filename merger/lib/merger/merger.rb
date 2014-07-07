require 'time'

class Merger::Merger
  def initialize
    publisher
    virtuoso_writer
    virtuoso_reader
  end
  
  def register_receiver
    receiver.subscribe(type: :movie_uri) { |movie_uri| merge(movie_uri) }
  end

  def merge(mapped_entity_uri)
    main_db_entity_uri = find_merged_entity(mapped_entity_uri)
    if main_db_entity_uri
      merge_into_entity new_entity_uri: mapped_entity_uri, existing_entity_uri: main_db_entity_uri
    else
      main_db_entity_uri = find_matching_entity(mapped_entity_uri)
      if main_db_entity_uri
        merge_into_entity new_entity_uri: mapped_entity_uri, existing_entity_uri: main_db_entity_uri
      else
        main_db_entity_uri = create_new_entity mapped_entity_uri: mapped_entity_uri
      end
      set_same_as_references main_db_uri: mapped_entity_uri, map_db_entry: main_db_entity_uri
    end
    update_provenience_information(main_db_entity_uri)
    # TODO: enqueue message with main_db_entity_uri onto mapped queue for consolidation
    # return main db uri
    main_db_entity_uri
  end

  def find_merged_entity(mapped_entity_uri)
    # TODO: Check if record in MainDB exists, that looks like { ?s sameAs mapped_entity_uri }
    # returns either merged_uri from MainDB entry or nil
  end

  def merge_into_entity(new_entity_uri:, existing_entity_uri:)
    # TODO: Per attribute from new record, merge into existing record
    # Therefore, get origin records the matching attribute in MainDB is built of from MapDB
    # Specific per attribute, choose merge strategy (vote, trustworthy source, ...)
    # Merge accordingly into record
  end

  def find_matching_entity(mapped_entity_uri)
    # TODO @Flo: Employ matcher to find matching entity within MainDB
    # returns matching entity's URI or nil
  end

  def create_new_entity(mapped_entity_uri:)
    # (Nico) Copy entity from MapDB into MainDB and update URIs to match MainDB schema
    copy_machine = Merger::CopyMachine.new mapped_entity_uri
    copy_machine.process # returns newly created entity URI
  end

  def set_same_as_references(main_db_uri:, map_db_entry:)
    # (Nico)
    virtuoso_writer.new_triple main_db_uri,
                               "#{Merger::Config.namespaces['owl']}sameAs",
                               map_db_entry, literal: false
  end

  def update_provenience_information(main_db_entity_uri)
    # TODO: Update provenience information within MainDB
    # Contains information like last merged at, ...
  end


  private
  def publisher
    @publisher ||= Merger::MsgPublisher.new
  end

  def receiver
    @receiver ||= Merger::MsgConsumer.new
  end

  def virtuoso_writer
    @virtuoso_writer ||= Merger::VirtuosoWriter.new
  end

  def virtuoso_reader
    @virtuoso_reader ||= Merger::VirtuosoReader.new
  end
end
