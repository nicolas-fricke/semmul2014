require 'date'
require 'levenshtein'
require 'set'

class Matcher::Matcher

  def initialize(evaluation=false)
    @evaluation = evaluation
    @virtuoso = Matcher::Virtuoso.new(evaluation)

    namespaces = YAML.load_file '../config/namespaces.yml'
    @types = namespaces['types']

    matching ||= YAML.load_file '../config/matching.yml'
    @weights = matching['weights']
    @thresholds = matching['thresholds']
    @control = matching['control']
    @settings = matching['settings']
  end

  def find(entity_uri)
    entity_triples = @virtuoso.get_triples(entity_uri, graph: 'mapped')
    identical = find_same entity_triples

    # try to find identical entities
    if identical
      identical
    else
      # try to find very similar entities
      find_thresh_matching entity_triples
    end
  end

  def find_same(entity_triples)
    if entity_triples.get_type == @types['movie_type']
      find_same_movie(entity_triples)
    end
  end

  def find_same_movie(entity_triples)
    # try to find certain indicators for identity of two movies
    same_entities = []
    # imdb id
    if imdb_id = entity_triples.get_imdb_id
      @virtuoso.get_movie_subjects_by_imdb(imdb_id).each do |same_ent|
        unless same_ent == entity_triples.subject
            same_entities << same_ent
        end
      end
    end
    # freebase_mid
    if freebase_m_id = entity_triples.get_fb_mid
      @virtuoso.get_movie_subjects_by_fb_mid(freebase_m_id).each do |same_mid_ent|
        unless same_mid_ent == entity_triples.subject
            same_entities << same_mid_ent
        end
      end
    end

    if same_entities.size > 1
      p ">>>>>>>> found more than one identical entities"
    end
    same_entities.first
  end

  def find_thresh_matching(entity_triples)
    matching = find_matching entity_triples
    unless matching.empty?
      if matching[-1][1] >= @thresholds['matching']
        return RDF::URI.new(matching[-1][0])
      end
    end
    nil
  end

  def find_best_matching(entity_triples)
    matching = find_matching(entity_triples)
    unless matching.empty?
      return RDF::URI.new(matching[-1][0])
    end
    nil
  end

  def find_matching(entity_triples)
    entity_type = entity_triples.get_type

    # get all entities with same type from virtuoso
    all_subjects = @virtuoso.get_entities_of_type entity_type

    all_matches = {}
    all_subjects.each do |subject_uri|
      unless entity_triples.subject == subject_uri
        # calculate match
        other_triples = @virtuoso.get_triples subject_uri # get from merged
        match = calculate_match entity_triples, other_triples, entity_type
        all_matches[subject_uri.to_s] = (match.nan? ? 0.0 : match)
      end
    end
    all_matches.sort_by {|_, match| match}
  end

  def calculate_match(a_triples, b_triples, type)
    case type
      when @types['movie_type']
        match_movie a_triples, b_triples
      when @types['person_type']
        person_match a_triples, b_triples
      when @types['director_type']
        person_match a_triples, b_triples
      when @types['organization_type']
        organization_match a_triples, b_triples
      when @types['performance_type']
        performance_match a_triples, b_triples
      else
        0.0
    end
  end

  def evaluation_same(a_triples, b_triples)
      # check if these two entities are the same

      # fb_mid
      a_fb_mid = a_triples.get_fb_mid()
      b_fb_mid = b_triples.get_fb_mid()
      unless a_fb_mid.nil? and b_fb_mid.nil?
          return a_fb_mid == b_fb_mid
      end

      # imdb_id
      a_imdb_id = a_triples.get_imdb_id()
      b_imdb_id = b_triples.get_imdb_id()
      unless a_imdb_id.nil? and b_imdb_id.nil?
          return  a_imdb_id == b_imdb_id
      end

      return false
  end


  def evaluation_match(a_uri, b_uri)
      a_triples = @virtuoso.get_triples(a_uri, graph: 'mapped')
      b_triples = @virtuoso.get_triples(b_uri, graph: 'mapped')
      entity_type = a_triples.get_type
      calculate_match(a_triples, b_triples, entity_type)
  end

  # def type_match(a,b)
  #   return 0.0 if a.nil? or b.nil?
  #   type_uri = RDF::URI.new "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  #   types_a = a.get_o type_uri
  #   types_b = b.get_o type_uri
  #
  #   # check if one of the types matches
  #   types_a.each do |type_a|
  #     types_b.each do |type_b|
  #       if type_a == type_b
  #         return true
  #       end
  #     end
  #   end
  #
  #   return false
  # end


  def organization_match(a,b)
    return 0.0 if a.nil? or b.nil?
    name_a = a.get_name.to_s
    name_b = b.get_name.to_s
    levenshtein_match name_a, name_b
  end

  def performance_match(a,b)
    return 0.0 if a.nil? or b.nil?

    # # match character
    # character_a = a.get_character().to_s
    # character_b = b.get_character().to_s
    # match_char = levenshtein_match(character_a, character_b)

    # match actor
    actor_a_uri = a.get_actor.first
    actor_b_uri = b.get_actor.first
    actor_a = @virtuoso.get_triples actor_a_uri, graph: 'mapped'
    actor_b = @virtuoso.get_triples actor_b_uri
    match_actor = person_match actor_a, actor_b

    # w_character = @weights['performance']['character']
    # w_actor = @weights['performance']['actor']

    # match_degree = (w_character * match_char) + (w_actor * match_actor)
    match_actor # TODO fix weights
  end

  def match_movie(a,b)
    return 0.0 if a.nil? or b.nil?
      # title
      title_a = a.get_name.to_s
      title_b = b.get_name.to_s
      title_match = levenshtein_match title_a, title_b

      # director

      # as we are merging from the mapped to merged graph, a comes from mapped b from merged
      # TODO make this more flexible, maybe pass the graph from which the triples were retrieved
      director_a = @virtuoso.get_triples a.get_director, graph: 'mapped'
      director_b = @virtuoso.get_triples b.get_director # TODO get_director returns only the first result, for some reason this
      use_director_match = true
      director_match = 0.0
      if director_a.nil? or director_b.nil?
          use_director_match = false
      else
          director_match = person_match director_a, director_b
      end


      # release date
      a_date = a.get_release_date
      b_date = b.get_release_date
      use_release_date = true # tmdb has no release date
      release_date_match = 0.0
      if a_date.nil? or b_date.nil?
          use_release_date = false
      else
          release_date_match = date_match a_date, b_date
      end

      # weights
      w_title = @weights['movie']['title']
      w_director = @weights['movie']['director']
      w_release = @weights['movie']['release']
      w_actors = @weights['movie']['actors']

      # todo: consolidate weight re-distribution
      calculate_fast_forward = @control['enable_ff']
      if !use_release_date and use_director_match
          w_title = w_title + (w_release * 0.5)
          w_director = w_director + (w_release * 0.5)
          w_release = 0
      elsif !use_director_match and use_release_date
          w_title += (w_director * 0.5)
          w_director = 0
      elsif !use_director_match and !use_release_date
          w_title += (w_director * 0.5)
          w_title += (w_release * 0.5)
          calculate_fast_forward = false
      end

      # clamp
      w_title = (w_title > 1 ? 1 : w_title)
      w_director = (w_director > 1 ? 1 : w_director)

      # fast-forward: check if already above threshold
      if calculate_fast_forward
          pre_match = 0
          pre_match += (w_title * title_match)
          pre_match += (w_release * release_date_match)
          pre_match += (w_director * director_match)

          if pre_match >= @thresholds['matching'] or pre_match <= @thresholds['ff_lower_bound']
              return pre_match
          end
      end

      # actors --> expensive
      use_actors_match = true
      actors_match = movie_actors_match a,b
      if actors_match.nil?
          actors_match = 0.0
          w_actors = 0.0
          # adjust other weights
          if !use_release_date and use_director_match
              w_title = w_title + (w_actors * 0.5)
              w_director = w_director + (w_actors * 0.5)
          elsif !use_director_match and use_release_date
              w_title += (w_actors * 0.5)
              w_release += (w_actors * 0.5)
          elsif !use_director_match and !use_release_date
              w_title += (w_actors * 0.5)
              w_title += (w_actors * 0.5)
          end
      end

      # clamp again
      w_title = (w_title > 1 ? 1 : w_title)
      w_director = (w_director > 1 ? 1 : w_director)
      w_release = (w_release > 1 ? 1 : w_release)

      match_degree = 0
      match_degree += (w_title * title_match)
      match_degree += (w_release * release_date_match)
      match_degree += (w_actors * actors_match)
      match_degree += (w_director * director_match)

    return match_degree
  end

  def person_match(a,b)

    # todo: family name, given name, multiple name fields

    return 0.0 if a.nil? or b.nil?
    # --> match names
    names_a = []
    if a
      a.get_alternative_names.each do |alt_name_a|
        names_a << alt_name_a.to_s
      end
      names_a << a.get_name.to_s.tr(",", " ")
    end

    names_b = []
    if b
      b.get_alternative_names.each do |alt_name_b|
        names_b << alt_name_b.to_s
      end
      names_b << b.get_name.to_s.tr(",", " ")
    end

    # todo: first_name, last_name, name will be given with different info
    name_alias_match = max_name_or_alias_match(names_a, names_b)

    # --> birthdate match
    birth_date_a = a.get_birthdate
    birth_date_b = b.get_birthdate
    birthdate_match = 0
    if !birth_date_a.nil? and !birth_date_b.nil?
      use_birthdate = true
      birthdate_match = date_match a.get_birthdate, b.get_birthdate #TODO
    else
      # if there is no birthdate, then we cannot use 0, as this would distort the result
      # in this case, we have to rely on the known information
      use_birthdate = false
    end

    # todo: birthplace match as string-match

    #birthplace_match = location_match(a[:birthplace],b[:birthplace])
    # todo: vector over works <-- expensive

    w_name_alias = @weights['person']['name_alias']
    w_birthdate = @weights['person']['birthdate']
    #w_birthplace = 0.2
    if !use_birthdate
        w_name_alias += w_birthdate
    end

    (w_name_alias*name_alias_match) + (w_birthdate*birthdate_match) # + (w_birthplace*birthplace_match)
  end

  def location_match(a,b)
    return 0.0 if a.nil? or b.nil?
    # location distance
    a_pos = {:lat  => a[:latitude], :long => a[:longitude]}
    b_pos = {:lat => b[:latitude], :long => b[:longitude]}
    distance_match = lat_long_match a_pos, b_pos

    # location name
    names_a = a[:aliases]
    names_a << a[:name]
    names_b = b[:aliases]
    names_b << b[:name]
    name_match = max_name_or_alias_match names_a, names_b

    # todo: country
    # todo: is contained by

    w_distance = 0.7
    w_name = 1-w_distance

    (w_distance * distance_match) + (w_name * name_match)
  end

  def levenshtein_match(a,b)
    a = a.downcase
    b = b.downcase
    normalizer = (a.size >= b.size ? a.size : b.size)
    1 - (levenshtein_distance(a,b) / normalizer.to_f)
  end

  def levenshtein_distance(s, t)
    m = s.length
    n = t.length
    return m if n == 0
    return n if m == 0
    d = Array.new(m+1) {Array.new(n+1)}

    (0..m).each {|i| d[i][0] = i}
    (0..n).each {|j| d[0][j] = j}
    (1..n).each do |j|
      (1..m).each do |i|
        d[i][j] = if s[i-1] == t[j-1]  # adjust index into string
                    d[i-1][j-1]       # no operation required
                  else
                    [ d[i-1][j]+1,    # deletion
                      d[i][j-1]+1,    # insertion
                      d[i-1][j-1]+1,  # substitution
                    ].min
                  end
      end
    end
    d[m][n]
  end


  def date_match(a,b)
    # make dates numerical
    if a >= b
      lower_date = b
      upper_date = a
    else
      lower_date = a
      upper_date = b
    end

    lower_date = DateTime.parse(lower_date.to_s)
    upper_date = DateTime.parse(upper_date.to_s)

    # calculate match
    std_dev = @settings['date_std_dev_d']
    mean = 0
    lower_date_i = 0
    difference_i = (upper_date - lower_date).to_i
    match_degree = normal_slope(difference_i, mean, std_dev)
    # ignore too small values
    if match_degree < 0.001
      return 0
    end

    match_degree
  end

  def lat_long_match(a,b)
    d = haversine_distance(a[:lat], a[:long], b[:lat], b[:long])
    normal_slope(d,0, 10) # sigma = 36 km
  end

  def max_name_or_alias_match(names_a, names_b)
    max_match = 0
      names_a.each do |name_a|
        names_b.each do |name_b|
          match = levenshtein_match(name_a.downcase, name_b.downcase)
          if match > max_match
            max_match = match
          end
        end
      end
    max_match
  end

  def name_alias_match(a,b)
    name_a = a[:name].downcase
    name_b = b[:name].downcase
    aliases_a = a[:aliases]
    aliases_b = b[:aliases]

    # name match
    name_match = levenshtein_distance name_a,name_b

    # find the max matching alias
    max_alias_a = nil
    max_alias_match = 0
    aliases_a.each do |alias_a|
      aliases_b.each do |alias_b|
        alias_match = levenshtein_match(alias_a, alias_b)
        if alias_match > max_alias_match
          max_alias_a = alias_a
          max_alias_match = alias_match
        end
      end
    end

    # calculate match
    name_weight = 0.25
    max_alias_weight = 1 - name_weight
    (name_weight * name_match) + (max_alias_weight * max_alias_match)
  end

  def movie_actors_match(a,b)
    a_actors = @virtuoso.get_actor_triples(a)
    b_actors = @virtuoso.get_actor_triples(b)

    if a_actors.empty? or b_actors.empty?
      return nil
    end

    # find match degrees between all actors
    all_matches = {}
    union_size = 0.0

    a_actors.each do |actor_a|
      b_actors.each do |actor_b|
        # todo: actor match that includes works
        if all_matches.has_key?([actor_b, actor_a])
          all_matches[[actor_a, actor_b]] = all_matches[[actor_b, actor_a]]
          next
        end

        match = person_match(actor_a, actor_b)
        all_matches[[actor_a, actor_b]] = match
        if match >= @thresholds['person_equivalence']
          union_size += 1
        end
      end
    end

    # use dice coefficient to measure similarity between actor sets
    # sim = 2|A union B|/|A|+|B|
    2 * union_size / (a_actors.size.to_f + b_actors.size.to_f)
  end


end

def haversine_distance(lat1,lon1,lat2,lon2)
	earth_radius = 6371 # km
	phi_1 = radians(lat1)
	phi_2 = radians(lat2)
	dphi = radians(lat2-lat1)
	dlambda = radians(lon2-lon1)
	a = Math.sin(dphi/2)**2 + (Math.cos(phi_1) * Math.cos(phi_2) * Math.sin(dlambda/2)**2)
	c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
	d = earth_radius * c
	return d

end

def euclidean_distance(x1,y1,x2,y2)
	return Math.sqrt(((x1-x2)**2)+(y1-y2))
end

def normal_slope(x, mu, sigma)
	pdf = Math.exp((-1.0*(x-mu)**2)/(2*sigma**2))
	return pdf
end

def radians(deg)
	return (deg/180) * Math::PI
end



