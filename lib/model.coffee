class Model
  constructor: (@ddb) ->
  create: (data, cb) ->
    @ddb.put @table, data, @keys, {}, cb
  query: (hash_val, n, cb) ->
    cond = {}
    cond[@keys[0]] = {EQ: [hash_val]}
    @_query cond, n, cb 
  query_before: (hash_val, range_val, n, cb) ->
    cond = {}
    cond[@keys[0]] = {EQ: [hash_val]}
    cond[@keys[1]] = {LT: [range_val]}
    @_query cond, n, cb
  query_after: (hash_val, range_val, n, cb) ->
    cond = {}
    cond[@keys[0]] = {EQ: [hash_val]}
    cond[@keys[1]] = {GT: [range_val]}
    @_query cond, n, cb
  query_latest: (hash_val, cb) ->
    cond = {}
    cond[@keys[0]] = {EQ: [hash_val]}
    cond[@keys[1]] = {LE: [new Date().getTime()]}
    @_query cond, 1, cb
  _query: (cond, n, cb) ->
    if @keys.length != 2
      return cb new Error "This table has no range key"
    cb = n if typeof n == 'function'
    opts = typeof n == 'function' && {} || {limit: n}
    @ddb.query @table, cond, null, opts, cb
  _validate_args: (index, hash_val, range_val, cb) ->
    keys = index || @keys
    if keys.length == 1 && cb
      throw new Error "Hash key is required only"
    if keys.length == 2 && typeof cb != 'function'
      throw new Error "No range key is specified"
  _get_cond: (index, hash_val, range_val) ->
    keys = index || @keys
    cond = {}
    cond[keys[0]] = hash_val
    cond[keys[1]] = range_val if keys.length == 2
    cond
  get_by: (index_name, hash_val, range_val, cb) ->
    index = @indexes[index_name]
    @_validate_args index, hash_val, range_val, cb
    cond = {}
    cond[index[0]] = {EQ: [hash_val]}
    cond[index[1]] = {EQ: [range_val]} if index.length == 2
    opts = index: index_name, limit: 1
    @ddb.query @table, cond, null, opts, (err, data) ->
      cb ||= range_val
      cb err, data?[0] || null
  get: (hash_val, range_val, cb) ->
    @_validate_args null, hash_val, range_val, cb
    cond = @_get_cond null, hash_val, range_val
    @ddb.get @table, cond, null, false, cb || range_val
  del: (hash_val, range_val, cb) ->
    @_validate_args null, hash_val, range_val, cb
    cond = @_get_cond null, hash_val, range_val
    @ddb.del @table, cond, null, cb || range_val
  update: (hash_val, range_val, data, cb) ->
    @_validate_args null, hash_val, range_val, cb
    cond = @_get_cond null, hash_val, range_val
    if typeof data == 'function'
      [data, cb] = [range_val, data]
    @ddb.update @table, cond, data, {}, cb
  inc: (hash_val, range_val, field, cb) ->
    @_validate_args null, hash_val, range_val, cb
    cond = @_get_cond null, hash_val, range_val
    if typeof field == 'function'
      [field, cb] = [range_val, field]
    opts = {}
    opts[field] = 1
    @ddb.incr @table, cond, opts, {}, (err, res) ->
      cb err, res?[field] || null
  putx: (hash_val, range_val, data, cb) -> 
    @_validate_args null, hash_val, range_val, cb
    cond = @_get_cond null, hash_val, range_val
    if typeof data == 'function'
      [data, cb] = [range_val, data]
    @ddb.add_set @table, cond, data, {}, cb
  delx: (hash_val, range_val, data, cb) -> 
    @_validate_args null, hash_val, range_val, cb
    cond = @_get_cond null, hash_val, range_val
    if typeof data == 'function'
      [data, cb] = [range_val, data]
    @ddb.del_set @table, cond, data, {}, cb


module.exports = Model
