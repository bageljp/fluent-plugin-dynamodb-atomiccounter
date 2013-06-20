# -*- coding: utf-8 -*-
module Fluent

  class DynamoDBAtomicCounterOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output('dynamodb_atomiccounter', self)

    include DetachMultiProcessMixin

    ITEM_LIMIT = 25
    CONTENT_SIZE_LIMIT = 1024*1024

    def initialize
      super
      require 'aws-sdk'
      require 'msgpack'
      require 'time'
      require 'uuidtools'
    end

    config_param :aws_key_id, :string
    config_param :aws_sec_key, :string
    config_param :proxy_uri, :string, :default => nil
    config_param :dynamo_db_table, :string
    config_param :dynamo_db_endpoint, :string, :default => nil
    config_param :time_format, :string, :default => nil
    config_param :detach_process, :integer, :default => 2
    config_param :count_key, :string, :default => 'path'

    def configure(conf)
      super
  
      @timef = TimeFormatter.new(@time_format, @localtime)
    end

    def start
      options = {
        :access_key_id      => @aws_key_id,
        :secret_access_key  => @aws_sec_key,
        :dynamo_db_endpoint => @dynamo_db_endpoint,
      }
      options[:proxy_uri] = @proxy_uri if @proxy_uri

      detach_multi_process do
        super

        begin
          restart_session(options)
        rescue ConfigError => e
          $log.fatal "ConfigError: Please check your configuration, then restart fluentd. '#{e}'"
          exit!
        rescue Exception => e
          $log.fatal "UnknownError: '#{e}'"
          exit!
        end
      end
    end

    def restart_session(options)
      config = AWS.config(options)
      @batch = AWS::DynamoDB::BatchWrite.new(config)
      @dynamo_db = AWS::DynamoDB.new(options)
      valid_table(@dynamo_db_table)
    end

    def valid_table(table_name)
      @table = @dynamo_db.tables[table_name]
      @table.load_schema
      @hash_key_value = @table.hash_key.name
    end

    def format(tag, time, record)
      record.to_msgpack
    end

    def write(chunk)
      batch_size = 0
      counts = Hash.new(0)

      chunk.msgpack_each {|record|
        count_key = record[@count_key] || ''

        next if count_key.empty?

        counts[count_key] += 1

        batch_size += record.to_s.length
        if counts.size >= ITEM_LIMIT || batch_size >= CONTENT_SIZE_LIMIT
          flush(counts)
          counts.clear
          batch_size = 0
        end
      }
      unless counts.empty?
        flush(counts)
      end
    end

    def flush(counts)
      #$log.info "counts=#{counts}"
      counts.each_pair do |k, v|
        item = @table.items[k]
        if item.exists?
          item.attributes.update {|u| u.add :counts => v }
        else
          item = @table.items.put(@hash_key_value => k, :counts => v)
        end
      end
    end
  end

end

