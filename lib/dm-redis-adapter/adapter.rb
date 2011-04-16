require 'redis'
require 'base64'

module DataMapper
  module Adapters
    class RedisAdapter < AbstractAdapter
      include DataMapper::Query::Conditions
      ##
      # Used by DataMapper to put records into the redis data-store: "INSERT" in SQL-speak.
      # It takes an array of the resources (model instances) to be saved. Resources
      # each have a key that can be used to quickly look them up later without
      # searching.
      #
      # @param [Enumerable(Resource)] resources
      #   The set of resources (model instances)
      #
      # @api semipublic
      def create(resources)
        resources.each do |resource|
          initialize_serial(resource, @redis.incr("#{resource.model.to_s.downcase}:#{redis_key_for(resource.model)}:serial"))
          @redis.sadd(key_set_for(resource.model), resource.key.join)
        end
        update_attributes(resources)
      end

      ##
      # Looks up one record or a collection of records from the data-store:
      # "SELECT" in SQL.
      #
      # @param [Query] query
      #   The query to be used to seach for the resources
      #
      # @return [Array]
      #   An Array of Hashes containing the key-value pairs for
      #   each record
      #
      # @api semipublic
      def read(query)
        fetched = records_for(query).map do |record|
          # Fetch stuff in hash about the actual record
          record.merge! @redis.hgetall("#{query.model.to_s.downcase}:#{query.model.key.map{|x| record[x.name.to_s]}.join(":")}")
        
          query.fields.each do |property|
            name = property.name.to_s
            value = record[name]

            # Integers are stored as Strings in Redis. If there's a
            # string coming out that should be an integer, convert it
            # now. All other typecasting is handled by datamapper
            # separately.
            record[name] = [Integer, Date].include?(property.primitive) ? property.typecast( value ) : value
          end
          record
        end
        query.filter_records(fetched)
        fetched
      end
        
      ##
      # Used by DataMapper to update the attributes on existing records in the redis
      # data-store: "UPDATE" in SQL-speak. It takes a hash of the attributes
      # to update with, as well as a collection object that specifies which resources
      # should be updated.
      #
      # @param [Hash] attributes
      #   A set of key-value pairs of the attributes to update the resources with.
      # @param [DataMapper::Collection] collection
      #   The collection object that should be used to find the resource(s) to update.
      #
      # @api semipublic
      def update(attributes, collection)
        attributes = attributes_as_fields(attributes)

        records_to_update = records_for(collection.query)
        records_to_update.each {|r| r.update(attributes)}
        update_attributes(collection)
      end

      ##
      # Destroys all the records matching the given query. "DELETE" in SQL.
      #
      # @param [DataMapper::Collection] collection
      #   The query used to locate the resources to be deleted.
      #
      # @return [Array]
      #   An Array of Hashes containing the key-value pairs for
      #   each record
      #
      # @api semipublic
      def delete(collection)
        collection.count.times do |x|
          record = collection[x]
          @redis.del("#{collection.query.model.to_s.downcase}:#{record[redis_key_for(collection.query.model)]}")
          @redis.srem(key_set_for(collection.query.model), record[redis_key_for(collection.query.model)])

          indexed_properties(record.model).each do |p|
            @redis.srem("#{collection.query.model.to_s.downcase}:#{p.name}:#{encode(record[p.name])}", record[redis_key_for(collection.query.model)])
          end
        end
      end

      private

      ##
      # Saves each resource to the redis data store
      #
      # @param [Array] Resources
      #   An array of resource to save
      #
      # @api private
      def update_attributes(resources)
        resources.each do |resource|
          model = resource.model
          attributes = resource.dirty_attributes

          indexed_properties(model).each do |property|
            @redis.sadd(index_key_set_for(model, property.name, resource[property.name.to_s]), resource.key.join(":").to_s)
          end

          properties_to_set = []
          properties_to_del = []

          fields = model.properties(self.name).select {|property| attributes.key?(property)}
          fields.each do |property|
            value = attributes[property]
            if value.nil?
              properties_to_del << property.name
            else
              properties_to_set << property.name << attributes[property]
            end
          end

          hash_key = "#{model.to_s.downcase}:#{resource.key.join(":")}"
          properties_to_del.each {|prop| @redis.hdel(hash_key, prop) }
          @redis.hmset(hash_key, *properties_to_set) unless properties_to_set.empty?
        end
      end
      
      def records_for(query)
        keys_for(query).map do |record_key_value|
          record = {}
          keys = record_key_value.to_s.split(":")
          query.model.key.to_a.each_with_index do |x, i|
            record[x.name.to_s] = keys[i]
          end
          record
        end
      end        
      ##
      #
      #
      #
      #
      #
      def keys_for(query)
        if query.conditions.empty?
          keys = []
          params = {}
          params[:limit] = [query.offset, query.limit] if query.limit

          if query.order && !query.order.empty?
            order = query.order.first
            params[:order] = order.operator.to_s
          else
            params[:order] = "nosort"
          end

          @redis.sort(key_set_for(query.model), params).each do |val|
            keys << val.to_i
          end
          keys
        else
          keys = keys_for_conditions(query)
          if keys.nil?
            keys = @redis.smembers(key_set_for(query.model))
          end
          keys
        end
      end

      ##
      # Returns the set of keys matching a comparison in a query's conditions
      #
      # @params [DataMapper::AbstractComparison]
      #  The comparison operator used in the conditions
      #
      # @return [Array]
      #  The list of keys to fetch which match this compariso}
      #
      # @api private
      def keys_for_conditions(query, condition = nil)
        condition ||= query.conditions
        case condition
        when nil then @redis.smembers(key_set_for(query.model))
        when AbstractOperation then keys_for_operation(query, condition)
        when AbstractComparison then keys_for_comparison(query, condition)
        else 
          debugger
          raise NotImplementedError
        end
      end

      def keys_for_operation(query, operation)
        case operation
        when NotOperation then keys_for_conditions(query, operation.first)
        when AndOperation then 
          keys = operation.operands.map {|op| keys_for_conditions(query, op)}
          if keys.any? {|x| x.nil? }
            nil
          else
            keys.reduce {|acc, keys| acc & keys}
          end
        when OrOperation then
        else
          debugger
          raise NotImplementedError
        end
      end

      def keys_for_comparison(query, comparison)
        case comparison
        when EqualToComparison then
          _get_keys_for_comparison(query, comparison, comparison.value)
        when InclusionComparison then
          comparison.value.map do |value|
            keys = _get_keys_for_comparison(query, comparison, value)
            return nil if keys.nil?
            keys
          end.flatten
        else
          raise NotImplementedError
        end
      end
      
      def _get_keys_for_comparison(query, comparison, value) 
        if comparison.relationship?
          find_relationship_matches(query, comparison, value)
        else
          find_property_index_matches(query, comparison, value)
        end
      end

      ##
      # Find a matching entry for a query
      #
      # @return [Array]
      #   Array of id's of all members matching the query
      # @api private
      def find_matches(query, operand)
        index_set = "#{query.model.to_s.downcase}:#{operand.subject.name}:#{encode(operand.value.to_s)}"
        unless operand.negated?
          @redis.smembers(index_set)
        else
          @redis.sdiff(key_set_for(query.model), index_set)
        end
      end

      ##
      # Find a matching entry for a query
      #
      # @return [Array]
      #   Array of id's of all members matching the query
      # @api private
      def find_property_index_matches(query, comparison, value)
        # Check for key based comparisons which are automatically indexed.
        if query.model.key.include?(comparison.subject)
          if @redis.sismember(key_set_for(query.model), value)
            affirmative = [value]
          end

          unless comparison.negated?
            return affirmative
          else
            return @redis.smembers(key_set_for(query.model)) - affirmative
          end
        end

        # Otherwise, check for :index => true on simple fields
        index_set = index_key_set_for(query.model, comparison.subject.name, value)
 
        # If this set doesn't exist, redis can't say, we do it in memory later
        return nil unless @redis.exists(index_set)

        unless comparison.negated?
          matches = @redis.smembers(index_set)
        else
          matches = @redis.sdiff(key_set_for(query.model), index_set)
        end
      end
      

      ## Finds ids based on a relationship comparison
      def find_relationship_matches(query, comparison, value)
        relationship = comparison.subject
        case relationship
          when DataMapper::Associations::ManyToOne::Relationship then
            # We're searching for an index on the child model. The redis set stores a list of the child models which
            # reference a particular parent, and encodes the *value* of the parent's key in the redis key of the set. Phew.
            child_foreign_key = relationship.child_key.map {|x| x.name }.join(":")
            parent_value = relationship.parent_key.map {|x| value[x.name]}.join(":")
            
            # The set of children who point to the parent
            affirmative_set = index_key_set_for(relationship.child_model, child_foreign_key, parent_value)

            # If this set doesn't exist, we can't do much, do it in memory later
            return nil unless @redis.exists(affirmative_set)

            # If the set does exist, return it for normal comparisons, and all children without the set for negated comparisons
            unless comparison.negated?
              return @redis.smembers(affirmative_set)
            else
              return @redis.sdiff(affirmative_set, key_set_for(relationship.child_model))
            end
          when DataMapper::Associations::ManyToMany::Relationship then
            # We're searching for an index on the child model
            child_foreign_key = relationship.child_key.map {|x| x.name }.join(":")
            # For each join model pointing to the child
            @redis.smembers("#{o.subject.via.child_model.to_s.downcase}:#{o.subject.via.child_key.first.name}:#{encode(child_key)}").each do |via|
              hash_key = "#{o.subject.via.child_model_name.to_s.downcase}:#{via}"
              keys << {o.subject.parent_key.first.name.to_s => @redis.hget(hash_key, o.subject.through.child_key.first.name)}
            end
          else
            raise NotImplemented
          end
      end

      ##
      # Creates a string representation for the keys in a given model
      #
      # @param [DataMapper::Model] model
      #   The query used to locate the resources to be deleted.
      #
      # @return [String]
      #   A string representation of the string key for this model
      #)
      # @api private
      def redis_key_for(model)
        model.key.collect {|k| k.name}.join(":")
      end
      
      ##
      # Return the key string for the set that contains all keys for a particular resource
      #
      # @return String
      #   The string key for the :all set
      # @api private
      def key_set_for(model)
        "#{model.to_s.downcase}:#{redis_key_for(model)}:all"
      end
      
      def index_key_set_for(model, field, value)
        "#{model.to_s.downcase}:#{field}:#{encode(value)}"
      end

      ##
      # Base64 encode a value as a string key for an index
      #
      # @return String
      #   Base64 representation of a value
      # @api private
      def encode(value)
        Base64.encode64(value.to_s).gsub("\n", "")
      end
      
      ##
      # Get properties on a model which have indexes
      def indexed_properties(model)
        #model.properties.select {|p| p.index }
        model.properties
      end

      ##
      # Make a new instance of the adapter. The @redis ivar is the 'data-store'
      # for this adapter.
      #
      # @param [String, Symbol] name
      #   The name of the Repository using this adapter.
      # @param [String, Hash] uri_or_options
      #   The connection uri string, or a hash of options to set up
      #   the adapter
      #
      # @api semipublic
      def initialize(name, uri_or_options)
        super
        @redis = Redis.new(@options)
      end
    end # class RedisAdapter

    const_added(:RedisAdapter)
  end # module Adapters
end # module DataMapper
