require 'set'

class Merger::CopyMachine
  attr_reader :map_db_uri, :main_db_uri

  def initialize(mapped_movie_uri, type: :generic, generate_new_uri: true)
    @map_db_uri = mapped_movie_uri
    @main_db_uri = if generate_new_uri
                     Merger::URIGenerator.new_uri for_type: type
                   else
                     mapped_movie_uri
                   end
  end

  def process
    copy_entity_with_literals @map_db_uri, @main_db_uri
    @main_db_uri
  end

  private
  def copy_entity_with_literals(map_db_uri, new_main_db_uri)
    copy_literals(map_db_uri, new_main_db_uri)
    copy_entities(map_db_uri, new_main_db_uri)
  end

  def copy_literals(map_db_uri, new_main_db_uri)
    belonging_literals =
        virtuoso_reader.get_objects_for subject: map_db_uri,
                                        predicate: :p,
                                        filter: 'isLiteral(?o)',
                                        result: [:p, :o]
    belonging_literals.each do |predicate, object_literal|
      virtuoso_writer.new_triple new_main_db_uri, predicate, object_literal,
                                 literal: true
    end
  end

  def copy_entities(map_db_uri, new_main_db_uri)
    belonging_uris =
        virtuoso_reader.get_objects_for subject: map_db_uri,
                                        predicate: :p,
                                        filter: 'isURI(?o)',
                                        result: [:p, :o]
    belonging_uris.each do |predicate, object_uri|
      merged_uri = Merger::Merger.merge(object_uri)
      virtuoso_writer.new_triple new_main_db_uri, predicate, merged_uri,
                                 literal: false
    end
  end


  def take_next_uri!
    taken = @found_uris.take(1)
    @found_uris.subtract taken unless taken.empty?
    taken.first
  end

  def map_db_uri_for(uri)
    @map_db_uris[uri] ||= Merger::URIGenerator.new_uri for_type: :generic
  end

  def virtuoso_writer
    @virtuoso_writer ||= Merger::VirtuosoWriter.new
  end

  def virtuoso_reader
    @virtuoso_reader ||= Merger::VirtuosoReader.new
  end
end

# MonkeyPatch Set to add take_one!
# class Set
#   def take!(args)
#     taken = self.take args
#     self.subtract taken
#     taken
#   end
#
#   def take_one!
#     self.take!(1).first
#   end
# end