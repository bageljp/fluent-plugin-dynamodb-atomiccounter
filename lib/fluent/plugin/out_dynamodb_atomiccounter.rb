# -*- coding: utf-8 -*-
module Fluent

  class DynamoDBAtomicCounterOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output('dynamodb_atomiccounter', self)

    include DetachMultiProcessMixin

    BATCHWRITE_ITEM_LIMIT = 25
    BATCHWRITE_CONTENT_SIZE_LIMIT = 1024*1024

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

    def configure(conf)
      super
  
      @timef = TimeFormatter.new(@time_format, @localtime)
      @hostname = `hostname`
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
      raise ConfigError, "Currently composite table is not supported." if @table.has_range_key?
      @hash_key_value = @table.hash_key.name
    end

    def format(tag, time, record)
      record.to_msgpack
    end

    def write(chunk)
      batch_size = 0
      count = Hash.new(0)

      chunk.msgpack_each {|record|
        json = record.to_json
        count[@hostname + json['path']] += 1
        batch_size += json.length
        if batch_records.size >= BATCHWRITE_ITEM_LIMIT || batch_size >= BATCHWRITE_CONTENT_SIZE_LIMIT
          flush(count)
          count.clear
          batch_size = 0
        end
      }
      unless count.empty?
        flush(count)
      end
    end

    def flush(count)
      $log.info "flush(count = #{count})"

      count.each_pair do |k, v|
        item = @table.items[k]
        if item.exists?
          item.attributes.update {|u| u.add :count => count }
        else
          item = @table.items.put(@hash_key_value => @hostname, :count => count)
        end
      end      
    end
  end

end

