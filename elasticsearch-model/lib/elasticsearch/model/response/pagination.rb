module Elasticsearch
  module Model
    module Response

      # Pagination for search results/records
      #
      module Pagination
        # Allow models to be paginated with the "kaminari" gem [https://github.com/amatsuda/kaminari]
        #
        module Kaminari
          def self.included(base)
            # Include the Kaminari modules: Configuration and paging
            #
            base.__send__ :include, ::Kaminari::ConfigurationMethods::ClassMethods
            base.__send__ :include, ::Kaminari::PageScopeMethods

            base.class_eval <<-RUBY, __FILE__, __LINE__ + 1
              # Define the `page` Kaminari method
              #
              def #{::Kaminari.config.page_method_name}(num=nil)
                @results  = nil
                @records  = nil
                @response = nil
                self.search.definition.update size: klass.default_per_page,
                                              from: klass.default_per_page * ([num.to_i, 1].max - 1)
                self
              end
            RUBY
          end

          # Returns the current "limit" (`size`) value
          #
          def limit_value
            case
              when search.definition[:body] && search.definition[:body][:size]
                search.definition[:body][:size]
              when search.definition[:size]
                search.definition[:size]
              else
                0
            end
          end

          # Returns the current "offset" (`from`) value
          #
          def offset_value
            case
              when search.definition[:body] && search.definition[:body][:from]
                search.definition[:body][:from]
              when search.definition[:from]
                search.definition[:from]
              else
                0
            end
          end

          # Set the "limit" (`size`) value
          #
          def limit(value)
            @results  = nil
            @records  = nil
            @response = nil
            search.definition.update :size => value
            self
          end

          # Set the "offset" (`from`) value
          #
          def offset(value)
            @results  = nil
            @records  = nil
            @response = nil
            search.definition.update :from => value
            self
          end

          # Returns the total number of results
          #
          def total_count
            results.total
          end
        end
      end

    end
  end
end