# encoding: utf-8

require 'open-uri'
require 'rexml/document'

# A type checker validates the type of given entities using type inference
# to reduce false positives.
#
# This type checker uses the DBpedia Type Completion Service,
# http://wifo5-21.informatik.uni-mannheim.de:8080/DBpediaTypeCompletionService/.
# See Paulheim and Bizer: Type Inference on Noisy RDF Data. 
#     In: International Semantic Web Conference (ISWC), 2013
class DBpediaCrawler::TypeChecker

private

  # URL of the service with place holders for the parameters
  SERVICE = "http://wifo5-21.informatik.uni-mannheim.de:8080/DBpediaTypeCompletionService" \
    + "/Service?resource=<<resource>>&threshold=<<threshold>>"

  # Create a string representing the threshold
  #   threshold: integer
  #   result: string
  def threshold_string(threshold)
    return "0.0" unless threshold.is_a? Integer
    if threshold >= 0 and threshold < 10
      return "0.0#{threshold}"
    elsif threshold >= 10 and threshold < 100
      return "0.#{threshold}"
    else
      return "0.0"
    end
  end

public

  # Create a new checker using the given threshold (in percent, 
  # 0 <= threshold < 100).
  #   config: hash
  def initialize(config)
    @threshold = threshold_string config["threshold"]
  end

  # Check whether the given entity has the given type (according to the type
  # inferer).
  #   entity: .to_s => URL
  #   entity: .to_s => URL
  #   result: boolean
  def entity_has_type?(entity, type)
    # prepare the URL
    url = SERVICE.clone
    url["<<resource>>"] = entity.to_s
    url["<<threshold>>"] = @threshold
    # request the result
    xml_string = URI.parse(url).read
    # receive the types
    types = []
    document = REXML::Document.new xml_string
    document.elements.each("//type") do |type_element|
      types << type_element.text
    end
    # check type
    return types.include?(type.to_s)
  end

end
