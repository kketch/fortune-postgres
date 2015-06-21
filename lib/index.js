import pg from 'pg'
import { primaryKeyTypes, typeMap, inputValue,
  inputRecord, outputRecord } from './helpers'


/**
 * PostgreSQL adapter.
 */
export default Adapter => class PostgreSQLAdapter extends Adapter {

  connect () {
    const { recordTypes, options, keys } = this

    if (!('url' in options))
      throw new Error(`A connection URL is required.`)

    let primaryKeyType = options.primaryKeyType || String

    if (!primaryKeyTypes.has(primaryKeyType)) {
      const types = []

      for (let type of primaryKeyTypes) types.push(type)

      throw new Error(`The primary key type must be one of ` +
        `${types.join(', ')}.`)
    }

    if (!('isNative' in options))
      options.isNative = true

    primaryKeyType = typeMap.get(primaryKeyType)

    return new Promise((resolve, reject) =>
      (options.isNative ? pg.native : pg)
      .connect(options.url, (error, client, done) => {
        if (error) return reject(error)

        this.client = client
        this.done = done

        return resolve()
      }))

    .then(() => new Promise((resolve, reject) =>
      this.client.query('set client_min_messages = error',
        error => error ? reject(error) : resolve())))

    .then(() => Promise.all(Object.keys(recordTypes)
    .map(type => new Promise((resolve, reject) => {
      const createTable = `create table if not exists "${type}" (` +
        [ `"${keys.primary}" ${primaryKeyType} primary key`,
        ...Object.keys(recordTypes[type]).map(field => {
          const fieldDefinition = recordTypes[type][field]
          const isArray = fieldDefinition[keys.isArray]
          const dataType = fieldDefinition[keys.type] ?
            typeMap.get(fieldDefinition[keys.type]) :
            primaryKeyType

          return `"${field}" ${dataType}${isArray ? '[]' : ''}`
        }) ].join(', ') + ')'

      this.client.query(createTable, error =>
        error ? reject(error) : resolve())
    }))))
  }


  disconnect () {
    const { options: { isNative } } = this
    delete this.client
    this.done()
    ; (isNative ? pg.native : pg).end()
    return Promise.resolve()
  }


  find (type, ids, options = {}) {
    // Handle no-op.
    if (ids && !ids.length) return super.find()

    const { recordTypes, client, keys } = this
    const fields = recordTypes[type]

    let columns = Object.keys(options.fields || {})
    columns = columns.length ?
      (columns.every(column => options.fields[column]) ?
      [ keys.primary, ...columns ] : [ keys.primary,
        ...Object.keys(fields).filter(field =>
          !columns.some(column => column === field)) ])
      .map(column => `"${column}"`).join(', ') : '*'

    const selectColumns = `select ${columns} from "${type}" `
    const sql = options.query || ''
    const parameters = []
    let index = 0
    let where = []
    let order = []
    let slice = ''

    const mapValue = value => {
      index++
      parameters.push(inputValue(value))
      return `$${index}`
    }

    if (ids) {
      where.push(`"${keys.primary}" in ` +
        `(${ids.map(() => {
          index++
          return `$${index}`
        }).join(', ')})`)
      parameters.push(...ids)
    }

    for (let field in options.match) {
      const value = options.match[field]
      const isArray = fields[field][keys.isArray]

      if (!isArray) {
        if (Array.isArray(value))
          where.push(`"${field}" in (${value.map(mapValue).join(', ')})`)
        else {
          index++
          parameters.push(inputValue(value))
          where.push(`"${field}" = $${index}`)
        }
        continue
      }

      // Array containment.
      if (Array.isArray(value))
        where.push(`"${field}" @> {${value.map(mapValue).join(', ')}}`)
      else {
        index++
        parameters.push(inputValue(value))
        where.push(`"${field}" @> {$${index}}`)
      }
    }

    where = where.length ? `where ${where.join(' and ')}` : ''

    for (let field in options.sort) {
      order.push(`"${field}" ` + (options.sort[field] === 1 ? 'asc' : 'desc'))
    }

    order = order.length ? `order by ${order.join(', ')}` : ''

    if (options.limit) slice += `limit ${options.limit} `
    if (options.offset) slice += `offset ${options.offset} `

    const findRecords = `${selectColumns} ${sql} ${where} ${order} ${slice}`

    // Parallelize the find method with count method.
    return Promise.all([
      new Promise((resolve, reject) =>
        client.query(findRecords, parameters.length ? parameters : null,
          (error, result) => error ? reject(error) : resolve(result))),
      new Promise((resolve, reject) =>
        client.query(`select count(*) from "${type}" ${sql} ${where}`,
          parameters.length ? parameters : null,
          (error, result) => error ? reject(error) : resolve(result)))
    ])

    .then(results => {
      const records = results[0].rows.map(outputRecord.bind(this, type))
      records.count = parseInt(results[1].rows[0].count, 10)
      return records
    })
  }


  create (type, records) {
    records = records.map(inputRecord.bind(this, type))

    const { recordTypes, keys, client, errors: { ConflictError } } = this

    // The sort order here doesn't really matter, as long as it's consistent.
    const orderedFields = Object.keys(recordTypes[type]).sort()

    const parameters = []
    let index = 0

    const createRecords = `insert into "${type}" (` + [
        `"${keys.primary}"`, ...orderedFields.map(field => `"${field}"`)
      ].join(', ') + `) values ` + records.map(record => {
        parameters.push(record[keys.primary],
          ...orderedFields.map(field => inputValue(record[field])))

        return `(${[ keys.primary, ...orderedFields ].map(() => {
          index++
          return `$${index}`
        }).join(', ')})`
      }).join(', ')

    return new Promise((resolve, reject) =>
      client.query(createRecords, parameters, error => {
        if (error) {
          // Cryptic SQL error state that means unique constraint violated.
          if (error.sqlState === '23505')
            return reject(new ConflictError(`Unique constraint violated.`))

          return reject(error)
        }

        return resolve(records.map(outputRecord.bind(this, type)))
      }))
  }


  update (type, updates) {
    const { client, keys } = this

    // This is a little bit wrong, it is only safe to update within a
    // transaction. It's not possible to put it all in one update statement,
    // since the updates may be sparse.
    return Promise.all(updates.map(update => new Promise((resolve, reject) => {
      const parameters = []
      let index = 0
      let set = []

      const mapValue = value => {
        index++
        parameters.push(inputValue(value))
        return `$${index}`
      }

      for (let field in update.replace) {
        const value = update.replace[field]
        index++
        if (Array.isArray(value)) parameters.push(value.map(inputValue))
        else parameters.push(inputValue(value))
        set.push(`"${field}" = $${index}`)
      }

      for (let field in update.push) {
        const value = update.push[field]
        index++

        if (Array.isArray(value)) {
          parameters.push(value.map(inputValue))
          set.push(`"${field}" = array_cat("${field}", $${index})`)
          continue
        }

        parameters.push(inputValue(value))
        set.push(`"${field}" = array_append("${field}", $${index})`)
      }

      for (let field in update.pull) {
        const value = update.pull[field]

        if (Array.isArray(value)) {
          // This array removal query is a modification from here:
          // http://www.depesz.com/2012/07/12/
          // waiting-for-9-3-add-array_remove-and-array_replace-functions/
          set.push(`"${field}" = array(select x from unnest("${field}") ` +
            `x where x not in (${value.map(mapValue).join(', ')}))`)
          continue
        }

        index++
        parameters.push(inputValue(value))
        set.push(`"${field}" = array_remove("${field}", $${index})`)
      }


      set = `set ${set.join(', ')}`

      index++
      parameters.push(update[keys.primary])
      const updateRecord = `update "${type}" ${set} ` +
        `where "${keys.primary}" = $${index}`

      client.query(updateRecord, parameters, (error, result) => error ?
        reject(error) : resolve(result.rowCount))
    })))
    .then(results => {
      return results.reduce((num, result) => {
        num += result
        return num
      }, 0)
    })
  }


  delete (type, ids) {
    const { keys, client } = this
    let index = 0

    const deleteRecords = `delete from "${type}"` + (ids ?
      ` where "${keys.primary}" in ` +
      `(${ids.map(() => {
        index++
        return `$${index}`
      }).join(', ')})` : '')

    return new Promise((resolve, reject) =>
      client.query(deleteRecords, ids ? ids : null,
        (error, result) => error ? reject(error) : resolve(result.rowCount)))
  }

}