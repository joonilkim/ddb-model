Model = require('../index').Model
Client = require('../index').Client
aws_secret = require './secret'

ddb = new Client aws_secret
class Product extends Model

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

schema_test = ->
  ddb.deleteTables schemas, (err) ->
    ddb.waitTable schemas[0], false, (err) ->
      ddb.createTables schemas, (err) ->
        ddb.waitTable schemas[0], true, (err) ->
          ddb.listTables (err, res) ->
            model_test()

model_test = ->
  product = new Product ddb, "test_products", ["name", "created_at"] 
  product.create {name: "iphone", created_at: new Date().getTime()}, (err) ->
    throw err if err
    product.create {name: "iphone", created_at: new Date().getTime()}, (err) ->
      throw err if err
      product.query "iphone", (err, res) ->
        throw err if err
        console.log res

schema_test()
