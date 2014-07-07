require 'securerandom'

class Merger::URIGenerator
  class << self
    def new_uri(for_type:, appended_to: nil)
      unless appended_to
        "#{base_namespace}/#{for_type.to_s}/#{SecureRandom.uuid}"
      else
        "#{appended_to.gsub /\/$/, ''}/#{SecureRandom.uuid}"
      end
    end

    def new_actor_uri
      new_uri for_type: :actor
    end

    def new_movie_uri
      new_uri for_type: :movie
    end

    private
    def base_namespace
      @base_namespace ||=
          Merger::Config.namespaces['schemas']['base'].gsub /\/$/, ''
    end
  end
end