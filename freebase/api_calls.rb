require './semmul2014_freebase_api'

film_properties = {
      "type"=> [],
      "id"=>"0bth54"
    }

Semmul2014FreebaseAPI.run_query film_properties do |result|
  result['type'].each do |x|
    puts x
  end
end

