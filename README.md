
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
Client = require('ddb-model').Client

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

ddb.deleteTables schemas, (err) ->
  ddb.waitTables schemas, false, (err) ->
    ddb.createTables schemas, (err) ->
      ddb.waitTables schemas, true, (err) ->
        ddb.listTables (err, res) ->
          console.log res

```

```js
Model = require('ddb-model').Model
Client = require('ddb-model').Client

ddb = new Client
  accessKeyId: ''
  secretAccessKey: ''
  region: ''
  maxRetries: 3

class Product extends Model
  table: 'test_products'
  keys: ['name', 'created_at']
  indexes: 
    gsi0: ['location']
    lsi0: ['name', 'location']

now = -> new Date().getTime()
product = new Product ddb
product.create {name: "iphone", created_at: now(), location: 'seoul'}, (err) ->
  throw err if err
  product.create {name: "iphone", created_at: now(), location: 'busan'}, (err) ->
    throw err if err
    product.query "iphone", (err, res) ->
      throw err if err
      console.log res
      product.get_by "gsi0", "busan", (err, res) ->
        throw err if err
        console.log res
        cb?.call @

```
  
* 현재시간부터 현재시간-24시간.. 역순

```
query_after 현재시간-24시간, -1, desc
```

* 최근 n개.. 역순

```
query_before 현재시간, n, desc
```

* t시간이전꺼 n개.. 역순

```
query_before t, n, desc
```

* 오래된거 n개.. 오래된순..

```
query_before 현재시간, n, asc
```

***
