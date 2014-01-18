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
    }
  ]
  ProvisionedThroughput:
    ReadCapacityUnits: 1
    WriteCapacityUnits: 1
} ]

schema_test = (cb) ->
  ddb.deleteTables schemas, (err) ->
    ddb.waitTables schemas, false, (err) ->
      ddb.createTables schemas, (err) ->
        ddb.waitTables schemas, true, (err) ->
          ddb.listTables (err, res) ->
            cb.call @

class Product extends Model
  table: 'test_products'
  keys: ['name', 'created_at']

model_test = ->
  product = new Product ddb
  product.create {name: "iphone", created_at: new Date().getTime()}, (err) ->
    throw err if err
    product.create {name: "iphone", created_at: new Date().getTime()}, (err) ->
      throw err if err
      product.query "iphone", (err, res) ->
        throw err if err
        console.log res

schema_test model_test
