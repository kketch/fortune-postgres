'use strict'

const crypto = require('crypto')
const randomBytes = crypto.randomBytes

const primaryKeyTypes = new Set([ String, Number ])

const postgresTypeMap = new WeakMap([
  [ String, 'text' ],
  [ Number, 'double precision' ],
  [ Boolean, 'boolean' ],
  [ Date, 'timestamp' ],
  [ Object, 'jsonb' ],
  [ Buffer, 'bytea' ]
])


module.exports = {
  primaryKeyTypes,
  postgresTypeMap,
  inputValue,
  inputRecord,
  outputRecord,
  getCode,
  generateQuery
}


function inputValue (value) {
  // PostgreSQL expects a special encoding for buffers.
  if (Buffer.isBuffer(value)) return `\\x${value.toString('hex')}`

  return value
}


function inputRecord (type, record) {
  const recordTypes = this.recordTypes
  const options = this.options
  const primaryKey = this.keys.primary
  const isArrayKey = this.keys.isArray
  const fields = recordTypes[type]
  const generatePrimaryKey = 'generatePrimaryKey' in options ?
    options.generatePrimaryKey : defaultPrimaryKey

  if (!(primaryKey in record) && generatePrimaryKey)
    record[primaryKey] = generatePrimaryKey(type)

  for (const field in fields) {
    const isArray = fields[field][isArrayKey]

    if (!(field in record)) {
      record[field] = isArray ? [] : null
      continue
    }

    record[field] = isArray ?
      record[field].map(inputValue) : inputValue(record[field])
  }

  return record
}


function outputRecord (type, record) {
  const recordTypes = this.recordTypes
  const primaryKey = this.keys.primary
  const isArrayKey = this.keys.isArray
  const typeKey = this.keys.type
  const denormalizedInverseKey = this.keys.denormalizedInverse
  const fields = recordTypes[type]
  const clone = {}

  for (const field in fields) {
    const fieldType = fields[field][typeKey]
    const fieldIsArray = fields[field][isArrayKey]
    const value = record[field]

    if (fields[field][denormalizedInverseKey]) {
      Object.defineProperty(clone, field, { value,
        writable: true, configurable: true })
      continue
    }

    if (fieldType &&
      (fieldType === Buffer || fieldType.prototype.constructor === Buffer) &&
      value && !Buffer.isBuffer(value)) {
      clone[field] = fieldIsArray ?
        value.map(outputBuffer) : outputBuffer(value)
      continue
    }

    if (field in record) clone[field] = value
  }

  clone[primaryKey] = record[primaryKey]

  return clone
}


function getCode (error) {
  return error.code || error.sqlState
}


function defaultPrimaryKey () {
  return randomBytes(15).toString('base64')
}


function outputBuffer (value) {
  if (Buffer.isBuffer(value)) return value
  return new Buffer(value.slice(2), 'hex')
}

function generateQuery (adapter, where, fields, options, parameters) {
  for (const key in options)
    switch (key) {
    case 'and':
      applyLogicalAnd(where, fields, options[key], parameters)
      return
    case 'or':
      applyLogicalOr(where, fields, options[key], parameters)
      return
    case 'not':
      applyLogicalNot(where, fields, options[key], parameters)
      return
    case 'range':
      applyRange(adapter, where, fields, options[key], parameters)
      break
    case 'match':
      applyMatch(adapter, where, fields, options[key], parameters)
      break
    case 'exists':
      applyExists(adapter, where, fields, options[key], parameters)
      break
    default:
    }

  return
}

function applyLogicalAnd (adapter, where, fields, clauses, parameters) {
  const outer = []
  for (let i = 0; i < clauses.length; i++) {
    const inner = []
    generateQuery(adapter, inner, fields, clauses[i], parameters)
    outer.push(inner)
  }
}

function applyMatch (adapter, where, fields, match, parameters) {
  const isArrayKey = adapter.keys.isArray
  for (const field in match) {
    const isArray = fields[field][isArrayKey]
    let value = match[field]

    if (!isArray) {
      if (Array.isArray(value))
        where.push(`"${field}" in (${value.map(mapValue).join(', ')})`)
      else {
        index++
        parameters.push(value)
        where.push(`"${field}" = $${index}`)
      }
      continue
    }

    // Array containment.
    if (!Array.isArray(value)) value = [ value ]
    where.push(`"${field}" @> array[${value.map(mapValueCast).join(', ')}]`)
  }
}

function applyExists (adapter, where, fields, exists) {
  const isArrayKey = adapter.keys.isArray
  for (const field in exists) {
    const isArray = fields[field][isArrayKey]
    const value = exists[field]

    if (!isArray) {
      where.push(`"${field}" ${value ? 'is not null' : 'is null'}`)
      continue
    }

    where.push(`coalesce(array_length("${field}", 1), 0) ${
      value ? '> 0' : '= 0'}`)
  }
}

function applyRange (adapter, where, fields, range, parameters) {
  for (const field in range) {
    const isArray = fields[field][isArrayKey]
    const value = range[field]

    if (!isArray) {
      if (value[0] != null) {
        index++
        parameters.push(value[0])
        where.push(`"${field}" >= $${index}`)
      }
      if (value[1] != null) {
        index++
        parameters.push(value[1])
        where.push(`"${field}" <= $${index}`)
      }
      continue
    }

    if (value[0] != null) {
      index++
      parameters.push(value[0])
      where.push(`coalesce(array_length("${field}", 1), 0) >= $${index}`)
    }
    if (value[1] != null) {
      index++
      parameters.push(value[1])
      where.push(`coalesce(array_length("${field}", 1), 0) <= $${index}`)
    }
  }
}


//mapValue && mapValueCast may need to be moved into buildWhereClause
    function mapValueCast (value) {
      index++
      parameters.push(value)

      let cast = ''

      if (Buffer.isBuffer(value))
        cast = '::bytea'
      else if (typeof value === 'number' && value % 1 === 0)
        cast = '::int'

      return `$${index}${cast}`
    }

    function mapValue (value) {
      index++
      parameters.push(value)
      return `$${index}`
    }
