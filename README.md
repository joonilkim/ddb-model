
## Install

```shell
npm install git+https://github.com/joonilkim/ddb-model.git --save
```

## Test

```shell
npm run-script test
or
npm test 
```

## Usage

```js
Client = require('ddb-model').client

ddb = new Client
  accessKeyId: ''
  secretAccessKey: ''
  region: ''
  maxRetries: 3

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

ddb.deleteTables schemas, (err) ->
  ddb.waitTables schemas, false, (err) ->
    ddb.createTables schemas, (err) ->
      ddb.waitTables schemas, true, (err) ->
        ddb.listTables (err, res) ->
          console.log res

```

```js
Model = require('ddb-model').model
Client = require('ddb-model').client

ddb = new Client
  accessKeyId: ''
  secretAccessKey: ''
  region: ''
  maxRetries: 3

class Product extends Model
  table: 'test_products'
  keys: ['name', 'created_at']

product = new Product ddb
product.create {name: "iphone", created_at: new Date().getTime()}, (err) ->
  throw err if err
  product.create {name: "iphone", created_at: new Date().getTime()}, (err) ->
    throw err if err
    product.query "iphone", (err, res) ->
      throw err if err
      console.log res
```
  
