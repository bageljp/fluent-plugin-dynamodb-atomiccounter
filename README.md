# Amazon DynamoDB AtomicCounter output plugin

##Installation

    $ fluent-gem install fluent-plugin-dynamodb-atomiccounter

##Configuration


###DynamoDB

First of all, you need to create a table in DynamoDB. It's easy to create via Management Console.

Specify table name, hash attribute name and throughput as you like. fluent-plugin-dynamodb_atomiccounter will load your table schema and write event-stream out to your table.

*currently supports only table with a primary key which has a string hash-key. (hash and range key is not supported.)*

### Fluentd

    <match dynamodb.**>
      type dynamodb_atomiccounter
      aws_key_id AWS_ACCESS_KEY
      aws_sec_key AWS_SECRET_ACCESS_KEY
      proxy_uri http://user:password@192.168.0.250:3128/
      dynamo_db_endpoint dynamodb.ap-northeast-1.amazonaws.com
      dynamo_db_table access_log
      count_key path
    </match>

 * **aws\_key\_id (required)** - AWS access key id.
 * **aws\_sec\_key (required)** - AWS secret key.
 * **proxy_uri (optional)** - your proxy url.
 * **dynamo\_db\_endpoint (required)** - end point of dynamodb. see  [Regions and Endpoints](http://docs.amazonwebservices.com/general/latest/gr/rande.html#ddb_region)
 * **dynamo\_db\_table (required)** - table name of dynamodb.
 * **count\_key** - key to be counted. default path.

##TIPS

###retrieving data

<table>
  <tr>
    <th>id (Hash Key)</th>
    <th>counts</th>
  </tr>
  <tr>
    <td>"/"</td>
    <td>"1458"</td>
  </tr>
  <tr>
    <td>"/sample.html"</td>
    <td>"847"</td>
  </tr>
  <tr>
    <td>"/about/"</td>
    <td>"373"</td>
  </tr>
</table>


###multi-region redundancy

As you know fluentd has **copy** output plugin.
So you can easily setup multi-region redundancy as follows.

    <match dynamo.**>
      type copy
      <store>
        type dynamodb_atomiccounter
        aws_key_id AWS_ACCESS_KEY
        aws_sec_key AWS_SECRET_ACCESS_KEY
        dynamo_db_table test
        dynamo_db_endpoint dynamodb.ap-northeast-1.amazonaws.com
        tag dynamo.count
        count_key path
        flush_interval 10s
      </store>
      <store>
        type dynamodb_atomiccounter
        aws_key_id AWS_ACCESS_KEY
        aws_sec_key AWS_SECRET_ACCESS_KEY
        dynamo_db_table test
        dynamo_db_endpoint dynamodb.ap-southeast-1.amazonaws.com
        tag dynamo.count
        count_key path
        flush_interval 10s
      </store>
    </match>

##TODO

 * test

##Copyright

<table> 
  <tr>
    <td>Copyright</td><td>Copyright (c) 2013 - Keisuke Kadoyama</td>
  </tr>
  <tr>
    <td>License</td><td>Apache License, Version 2.0</td>
  </tr>
</table>
