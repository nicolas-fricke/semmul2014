require './semmul2014_freebase_api'

film_properties = {	"id"=> "/film/film",
            "properties"=> [{
                        "name"=> nil
                    }],
            "type"=> "/type/type"
          }

Semmul2014FreebaseAPI.run_query film_properties do |result|
  puts "number of properties for /film/film type: #{result['properties'].size}"
  puts "properties:\n\n"
  result['properties'].each do |property|
    puts property['name']
  end
end

topic_properties = {	"id"=> "/common/topic",
"properties"=> [{
"name"=> nil
}],
"type"=> "/type/type"
}


Semmul2014FreebaseAPI.run_query topic_properties do |result|
  puts "number of properties for /common/topic type: #{result['properties'].size}"
  puts "properties:\n\n"
  result['properties'].each do |property|
    puts property['name']
  end
end

