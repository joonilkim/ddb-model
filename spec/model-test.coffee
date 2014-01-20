Model = require('../index').Model
Client = require('../index').Client
aws_secret = require './secret'

ddb = new Client aws_secret

schemas = [ {
  TableName: 'test_products'
  Attributes: [
    { AttributeName: 'name',\
      AttributeType: 'S',\
      KeyType: 'HASH'
    },
    { AttributeName: 'created_at',\
      AttributeType: 'N',\
      KeyType: 'RANGE'
    },
    { AttributeName: 'location',\
      AttributeType: 'S'
    }
  ],
  GlobalSecondaryIndexes: [ {
    IndexName: 'gsi0' 
    KeySchema: [ 
      {
        AttributeName: 'location'
        KeyType: 'HASH'
      }
    ]
    Projection:
      ProjectionType: 'INCLUDE'
      NonKeyAttributes: ['name', 'created_at']
    ProvisionedThroughput:
      ReadCapacityUnits: 1
      WriteCapacityUnits: 1
  } ],
  LocalSecondaryIndexes: [ {
    IndexName: 'lsi0' 
    KeySchema: [ 
      {
        AttributeName: 'name'
        KeyType: 'HASH'
      },
      {
        AttributeName: 'location'
        KeyType: 'RANGE'
      }
    ]
    Projection:
      ProjectionType: 'INCLUDE'
      NonKeyAttributes: ['created_at']
  } ],
  ProvisionedThroughput:
    ReadCapacityUnits: 1
    WriteCapacityUnits: 1
} ]

schema_test = (cb) ->
  ddb.createTables schemas, (err) ->
    throw err if err
    ddb.waitTables schemas, true, (err) ->
      throw err if err
      ddb.listTables (err, res) ->
        throw err if err
        cb?.call @

clear_schemas = (cb) ->
  ddb.deleteTables schemas, (err) ->
    cb?.call @

class Product extends Model
  table: 'test_products'
  keys: ['name', 'created_at']
  indexes: 
    gsi0: ['location']
    lsi0: ['name', 'location']

product = new Product ddb

prod0 =
  name: 'iphone'
  created_at: new Date().getTime()
  location: 'seoul'
  
prod1 =
  name: 'iphone'
  created_at: new Date().getTime()+1
  location: 'busan'

prod2 =
  name: 'iphone'
  created_at: new Date().getTime()+2
  location: 'seoul'


insert_test = (cb) ->
  product.create prod0, (err) ->
    throw err if err
    product.create prod1, (err) ->
      throw err if err
      product.create prod2, (err) ->
        throw err if err
        cb()

query_test = (cb) ->
  product.query "iphone", (err, res) ->
    throw err if err
    console.log res
    product.get_by "gsi0", "busan", (err, res) ->
      throw err if err
      console.log res
      cb?.call @

schema_test ->
  insert_test ->
    query_test ->
      clear_schemas()

