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

  def merge(mapped_movie_uri)
    main_db_entity_uri = find_merged_entity(mapped_movie_uri)
    if main_db_entity_uri
      merge_into_entity new_entity_uri: mapped_movie_uri, existing_entity_uri: main_db_entity_uri
    else
      main_db_entity_uri = find_matching_entity(mapped_movie_uri)
      if main_db_entity_uri
        merge_into_entity new_entity_uri: mapped_movie_uri, existing_entity_uri: main_db_entity_uri
      else
        main_db_entity_uri = create_new_entity new_entity_uri: mapped_movie_uri
      end
      set_same_as_references new_entity_uri: mapped_movie_uri, existing_entity_uri: main_db_entity_uri
    end
    update_provenience_information(main_db_entity_uri)
    # TODO: enqueue message with main_db_entity_uri onto mapped queue for consolidation
  end

  def find_merged_entity(mapped_movie_uri)
    # TODO: Check if record in MainDB exists, that looks like { ?s sameAs mapped_movie_uri }
    # returns either merged_uri from MainDB entry or nil
  end

  def merge_into_entity(new_entity_uri:, existing_entity_uri:)
    # TODO: Per attribute from new record, merge into existing record
    # Therefore, get origin records the matching attribute in MainDB is built of from MapDB
    # Specific per attribute, choose merge strategy (vote, trustworthy source, ...)
    # Merge accordingly into record
  end

  def find_matching_entity(mapped_movie_uri)
    # TODO: Employ matcher to find matching entity within MainDB
    # returns matching entity's URI or nil
  end

  def create_new_entity(new_entity_uri:)
    # TODO: Copy entity from MapDB into MainDB and update URIs to match MainDB schema
    # return newly created entity URI
  end

  def set_same_as_references(new_entity_uri:, existing_entity_uri:)
    # TODO: Set same_as reference within MainDB representing { existing_entity_uri sameAs new_entity_uri }
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
