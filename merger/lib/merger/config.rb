require 'yaml'

class Merger::Config
  class << self
    def secrets
      @secrets ||= YAML.load_file(
            File.expand_path('../../../../config/secrets.yml', __FILE__)
          )
    end

    def namespaces
      @namespaces ||= YAML.load_file(
            File.expand_path('../../../../config/namespaces.yml', __FILE__)
          )
    end
  end
end