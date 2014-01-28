aws = require 'aws-sdk'

class Client
  constructor: (cred) ->
    aws.config.update cred
    @ddb = new aws.DynamoDB

  # insert if not exist, error if exist
  # usage: put(tb, {xx:1, yy:2}, [xx], {})
  put: (table, item, keys, ops, cb) ->
    params = {}
    params.TableName = table
    params.Item = o2ddb item
    params.Expected = {}
    for k in keys
      params.Expected[k] = {Exists: false}
    merge params, ops
    self = @
    @ddb.putItem params, (err, data) ->
      if err
        cb?.call self, err
      else
        cb?.call self, null, if data.Attributes then \
          ddb2o data.Attributes else {}

  # insert if not exist, update if exist
  # usage: upsert(tb, {xx:1, yy:2}, {})
  upsert: (table, item, ops, cb) ->
    @put table, item, [], ops, cb

  # usage: get(tb, {xx:1}, [xx,yy], null, false)
  # {xx: {}}
  get: (table, key, attrs, consistent, cb) ->
    params = {}
    params.TableName = table
    params.Key = o2ddb key
    params.AttributesToGet = attrs if attrs
    params.ConsistentRead = true if consistent
    self = @
    @ddb.getItem params, (err, data) ->
      if err
        cb?.call self, err
      else
        cb?.call self, null, data.Item && ddb2o data.Item || null

  count: (table, cond, ops, cb) ->
    ops.Select = 'COUNT'
    @query table, cond, null, ops, cb

  # cond = {xx: {EQ: ['a']}}
  query: (table, cond, attrs, ops, cb) ->
    params = {}
    params.TableName = table
    params.KeyConditions = o2ddb_cond(cond)
    params.AttributesToGet = attrs if attrs
    params.IndexName = ops.index if ops.index
    params.Limit = ops.limit if ops.limit && ops.limit > 0
    params.ExclusiveStartKey = ops.start_key if ops.start_key
    params.ScanIndexForward = ops.desc if ops.desc
    params.ConsistentRead = ops.consistent if ops.consistent
    self = @
    @ddb.query params, (err, data) ->
      if err
        cb?.call self, err
      else
        cb?.call self, null, data.Items.map (x) -> ddb2o(x)

  scan_count: (table, cond, ops, cb) ->
    ops.Select = 'COUNT'
    @scan table, cond, null, ops, cb

  # cond = {xx: {'EQ': ['a']}}
  scan: (table, cond, attrs, ops, cb) ->
    params = {}
    params.TableName = table
    params.ScanFilter = o2ddb_cond(cond) if cond
    params.AttributesToGet = attrs if attrs
    params.Limit = ops.limit if ops.limit
    params.ExclusiveStartKey = ops.start_key if ops.start_key
    self = @
    @ddb.scan params, (err, data) ->
      if err
        cb?.call self, err
      else
        cb?.call self, null, data.Items.map (x) -> ddb2o(x)

  del: (table, key, ops, cb) ->
    params = {}
    params.TableName = table
    params.Key = o2ddb key
    merge params, ops
    self = @
    @ddb.deleteItem params, (err, data) ->
      if err
        cb?.call self, err
      else
        cb?.call self, null, if data.Attributes then \
          ddb2o data.Attributes else {}

  # update if exist, error if not exist
  # usage: update(tb, {xx:1}, {yy:2,zz:3})
  update: (table, key, attr, ops, cb) ->
    params = {}
    params.TableName = table
    params.Key = o2ddb key
    params.Expected = {}
    for k,v of key
      params.Expected[k] =
        Exists: true
        Value: ddbtype(v)
    params.AttributeUpdates = {}
    for k,v of attr
      params.AttributeUpdates[k] =
        Value: ddbtype(v)
        Action: 'PUT'
    merge params, ops
    self = @
    @ddb.updateItem params, (err, data) ->
      if err
        cb?.call self, err
      else
        cb?.call self, null, if data.Attributes then \
          ddb2o data.Attributes else {}

  # usage: incr(tb, {xx:1}, {yy:-1})
  incr: (table, key, attr, ops, cb) ->
    params = {}
    params.ReturnValues = 'UPDATED_NEW'
    params.AttributeUpdates = {}
    for k,v of attr
      if typeof v != 'number'
        throw "number type is only allowed" 
      params.AttributeUpdates[k] =
        Value: ddbtype(v)
        Action: 'ADD'
    merge params, ops
    @update table, key, {}, params, cb

  # usage: add_set(tb, {xx:1}, {yy:[a]})
  add_set: (table, key, attr, ops, cb) ->
    params = {}
    params.ReturnValues = 'UPDATED_NEW'
    params.AttributeUpdates = {}
    for k,v of attr
      params.AttributeUpdates[k] =
        Value: ddbtype(Array.isArray(v) && v || [v])
        Action: 'ADD'
    merge params, ops
    @update table, key, {}, params, cb

  # usage: del_set(tb, {xx:1}, {yy:[a]})
  del_set: (table, key, attr, ops, cb) ->
    params = {}
    params.ReturnValues = 'UPDATED_NEW'
    params.AttributeUpdates = {}
    for k,v of attr
      params.AttributeUpdates[k] =
        Value: ddbtype(Array.isArray(v) && v || [v])
        Action: 'DELETE'
    merge params, ops
    @update table, key, {}, params, cb

  # RequestItems: {tb1: [PutRequest: {Item: {xx: {'S':'a'}}}, DelRequest: ...]}
  # usage: mput {tb1: [{xx: 'a', yy:1},{xx: 'b'}}], tb2: [...]}
  # if has data.UnprocessedItems, do mput(null, data.UnprocessedItems, cb)
  mput: (item, ops, cb) ->
    params = {RequestItems: {}}
    ri = params.RequestItems
    for tb, data of item
      ri[tb] = data.map (d) -> {PutRequest: {Item: o2ddb(d)} }
    merge params, ops
    @ddb.batchWriteItem params, cb

  # usage: mdel {tb1: [{xx: 'a', yy:1},{xx: 'b'}}], tb2: [...]}
  # if has data.UnprocessedItems, do mdel(null, data.UnprocessedItems, cb)
  mdel: (item, ops, cb) ->
    params = {RequestItems: {}}
    ri = params.RequestItems
    for tb, data of item
      ri[tb] = data.map (d) -> {DeleteRequest : {Key: o2ddb(d)} }
    merge params, ops
    @ddb.batchWriteItem params, cb

  # toputs: tb: [{xx:1,yy:2}], tb2: []
  # todels: tb: [{xx:1,yy:2}], tb2: []
  batch: (toputs, todels, ops, cb) ->
    params = {RequestItems: {}}
    ri = params.RequestItems
    for tb, data of toputs
      for d in data
        (ri[tb] ||= []).push {PutRequest : {Item: o2ddb(d)} }
    for tb, data of todels
      for d in data
        (ri[tb] ||= []).push {DeleteRequest : {Key: o2ddb(d)} }
    merge params, ops
    self = @
    @ddb.batchWriteItem params, (err, res) ->
      if err
        cb?.call self, err
      else if res.UnprocessedItems && \
          Object.keys(res.UnprocessedItems).length > 0
        params = RequestItems: res.UnprocessedItems
        process.nextTick (-> @ddb.batchWriteItem(params, cb)).bind(self)
      else
        cb?.call self, null, res
    

  # RequestItems: {tb1: {Keys: [], AttributesToGet: [], ConsistentRead: true}
  # usage: {tb1: {keys: [{xx:1}], attr: [xx,yy], consistent: true} }
  # if has data.UnprocessedKeys , do mget(null, data.UnprocessedKeys, cb)
  # Responses: {tb1: [{xx: {'S':'a'}} ] }
  # { UnprocessedKeys: {}, res: { users: [], emails: [] } }
  mget: (item, ops, cb) ->
    params = {RequestItems: {}}
    ri = params.RequestItems
    for tb, data of item
      ri[tb] = {}
      ri[tb].Keys = data.keys.map (k) -> o2ddb(k)
      ri[tb].AttributesToGet = data.attr if data.attr
      ri[tb].ConsistentRead = data.consistent if data.consistent
    @ddb.batchGetItem params, (err, data) ->
      if err
        cb? err
      else
        res =
          UnprocessedKeys: data.UnprocessedKeys
          res: {}
        for tb, attrs of data.Responses
          res.res[tb] = attrs.map (attr) -> ddb2o(attr)
        cb? null, res
  timeout: (ms, cb) -> setTimeout cb, ms
  _dup: (o) -> JSON.parse JSON.stringify(o)
  waitTables: (schemas, exists, cb) ->
    return cb?.call @ if schemas.length == 0
    self = @
    @waitTable schemas[0], exists, (err) -> 
      targets = err && schemas || self._dup(schemas).slice(1)
      self.waitTables(targets, exists, cb)
  waitTable: (schema, exists, cb) ->
    self = @
    @ddb.describeTable TableName: schema.TableName, (err, res) ->
      process.stdout.write '.'
      throw err if exists && err
      if !exists && err || res.Table.TableStatus == 'ACTIVE'
        cb?.call self, null
      else
        self.timeout 2000, -> self.waitTable(schema, exists, cb)
  deleteTables: (schemas, cb) ->
    finished = 0
    last_err = null
    self = @
    for s in schemas
      @ddb.deleteTable TableName: s.TableName, (err, res) ->
        finished++
        last_err = err if err
        cb?.call self, last_err if finished == schemas.length
  createTables: (schemas, cb) ->
    finished = 0
    last_err = null
    self = @
    for s in schemas
      x =
        TableName: s.TableName
        AttributeDefinitions: []
        KeySchema: []
      if s.GlobalSecondaryIndexes
        x.GlobalSecondaryIndexes = s.GlobalSecondaryIndexes
      if s.LocalSecondaryIndexes
        x.LocalSecondaryIndexes = s.LocalSecondaryIndexes
      x.ProvisionedThroughput = s.ProvisionedThroughput || 
        { ReadCapacityUnits: 1, WriteCapacityUnits: 1 }
      for attr in s.Attributes
        x.AttributeDefinitions.push 
          AttributeName: attr.AttributeName
          AttributeType: attr.AttributeType
        if attr.KeyType
          x.KeySchema.push
            AttributeName: attr.AttributeName
            KeyType: attr.KeyType

      @ddb.createTable x, (err, res) ->
        finished++
        last_err = err if err
        cb?.call self, err if finished == schemas.length
  listTables: (cb) ->
    @ddb.listTables cb

merge = (o1, o2) ->
  return o1 unless o2
  for k,v of o2
    o1[k] = v
  o1

# {xx: {'EQ': ['a']}} =>
# {xx: {AttributeValueList: [{'S':'a'}], ComparisonOperator: 'EQ'}}
o2ddb_cond = (conds) ->
  res = {}
  for k,cond of conds
    for op, val of cond
      res[k] = 
        ComparisonOperator: op
        AttributeValueList: val.map (v) -> ddbtype(v)
  res

# {x:'a', y:2} => {x: {'S':'a'}, y: {'N':'2'}}
o2ddb = (item) ->
  res = {}
  for k,v of item
    res[k] = ddbtype(v)
  res

# 'a' => {'S': 'a'}
ddbtype = (val) ->
  if typeof val == 'number'
    return {N: val.toString()}
  if typeof val == 'string'
    return {S: val}
  if Array.isArray val
    if val.length > 0 && typeof val[0] == 'number'
      return {NS:val.map (x) -> x.toString()}
    return {SS:val}
  throw "wrong type: #{val}"

# {xx: {'S':'a'}, yy: {'N':'1'}} => {xx:'a', yy:1}
ddb2o = (item) ->
  res = {}
  for k,v of item
    res[k] = if v.S
      v.S
    else if v.N
      parseFloat v.N
    else if v.SS
      v.SS
    else if v.NS
      v.NS.map (x) -> parseFloat x
  res

module.exports = Client

