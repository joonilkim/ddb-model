class Model
  constructor: (@ddb, @table, @keys, @opts) ->
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
      return cb.call(new Error "This table has no range key")
    cb = n if typeof n == 'function'
    opts = typeof n == 'function' && {} || {limit: n}
    @ddb.query @table, cond, null, opts, cb
  _validate_args: (hash_val, range_val, cb) ->
    if @keys.length == 1 && cb
      throw new Error "Hash key is required only"
    if @keys.length == 2 && typeof cb != 'function'
      throw new Error "No range key is specified"
  _get_cond: (hash_val, range_val) ->
    cond = {}
    cond[@keys[0]] = hash_val
    cond[@keys[1]] = range_val if @keys.length == 2
    cond
  get: (hash_val, range_val, cb) ->
    @_validate_args hash_val, range_val, cb
    cond = @_get_cond hash_val, range_val
    @ddb.get @table, cond, null, false, cb || range_val
  del: (hash_val, range_val, cb) ->
    @_validate_args hash_val, range_val, cb
    cond = @_get_cond hash_val, range_val
    @ddb.del @table, cond, null, false, cb || range_val
  update: (hash_val, range_val, data, cb) ->
    @_validate_args hash_val, range_val, cb
    cond = @_get_cond hash_val, range_val
    @ddb.update @table, cond, data, cb || range_val
  inc: (hash_val, range_val, field, cb) ->
    @_validate_args hash_val, range_val, cb
    cond = @_get_cond hash_val, range_val
    field = range_val if typeof field == 'function'
    opts = {}
    opts[field] = 1
    @ddb.incr @table, cond, opts, cb
  putx: (hash_val, range_val, data, cb) -> 
    @_validate_args hash_val, range_val, cb
    cond = @_get_cond hash_val, range_val
    data = range_val if typeof data == 'function'
    @ddb.add_set @table, cond, data, cb
  delx: (hash_val, range_val, data, cb) -> 
    @_validate_args hash_val, range_val, cb
    cond = @_get_cond hash_val, range_val
    data = range_val if typeof data == 'function'
    @ddb.del_set @table, cond, data, cb


module.exports = Model
