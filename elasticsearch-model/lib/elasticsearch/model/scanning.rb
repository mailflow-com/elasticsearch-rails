require 'elasticsearch/model/searching'

module Elasticsearch
  module Model
    module Scanning
      class ScanRequest < Elasticsearch::Model::Searching::SearchRequest
        DEFAULT_OPTIONS = {scroll: '5m', size: 100, search_type: 'scan'}

        def initialize(klass, query_or_payload, options = {})
          super(klass, query_or_payload, DEFAULT_OPTIONS.merge(options))
        end

        def execute!
          client = klass.client
          scan_results = client.search(definition)
          scroll_id = scan_results['_scroll_id']
          hits = []

          while (scroll_results = client.scroll(scroll_id: scroll_id, scroll: options[:scroll])) && !scroll_results['hits']['hits'].blank? do
            scroll_id = scroll_results['_scroll_id']
            hits << scroll_results['hits']['hits']
          end

          scan_results['hits']['hits'] = hits.flatten
          scan_results
        end
      end

      module ClassMethods
        def scan(query_or_payload, options = {})
          search = ScanRequest.new(self, query_or_payload, options)
          Response::Response.new(self, search)
        end
      end
    end
  end
end
