const OmniDbNative = require('bindings')('omnidb')

const log = require('./log.js')
const debugLog = log.debugLog
const genLogId = log.genLogId
const getLogMsg = log.getLogMsg

const { isAS400, getAS400Schemas, getAS400CurrentSchema } = require('./as400.js')

const {
  isPostgres,
  setPostgresSchemas,
  setPostgresTables,
  setPostgresColumns,
  getPostgresQuery,
  getPostgresCurrentSchema,
} = require('./postgres.js')

const {
  isMySQL,
  getMySQLSchemas,
  getMySQLTables,
  getMySQLColumns,
  getMySQLPrimaryKeys,
  getMySQLQuery,
  getMySQLCurrentSchema,
} = require('./mysql.js')

const {
  isMSSQL,
  isFreetds,
  //getMSSQLSchemas,
  getMSSQLTables,
  /*
  getMSSQLColumns,
  getMSSQLPrimaryKeys,
  getMSSQLQuery,
  */
  getMSSQLCurrentSchema,
} = require('./mssql.js')

class OmniDb {
  constructor() {
    this._native = new OmniDbNative()
    this._dbms = ''
    this._driver = ''
  }

  static set debug(value) {
    log.isDebug = value
  }

  static get debug() {
    return log.isDebug
  }

  drivers() {
    return new Promise((resolve) => {
      const lid = genLogId()
      debugLog('drivers', '<fid>', lid)

      const resJson = this._native.drivers()

      debugLog('drivers', '<res>', resJson, '<fid>', lid)
      resolve(JSON.parse(resJson))
    })
  }
  connect(connectionString) {
    return new Promise((resolve) => {
      const lid = genLogId()
      debugLog('connect', '<cnn>', getLogMsg(connectionString), '<fid>', lid)
      const result = this._native.connect(connectionString)
      if (result) {
        this._dbms = this._native.dbms()
        this._driver = this._native.driver()
      }
      debugLog('connect', '<res>', result, '<fid>', lid)
      resolve(result)
    })
  }
  disconnect() {
    return new Promise((resolve) => {
      const lid = genLogId()
      debugLog('disconnect', '<db>', this.dbms(), '<fid>', lid)
      const result = this._native.disconnect()
      if (result) {
        this._dbms = ''
        this._driver = ''
      }
      debugLog('disconnect', '<res>', result, '<fid>', lid)
      resolve(result)
    })
  }
  driver() {
    return this._driver
  }
  dbms() {
    return this._dbms
  }
  schemas(condition = {}) {
    return new Promise((resolve) => {
      const lid = genLogId()

      debugLog('schemas', '<cond>', JSON.stringify(condition), '<db>', this.dbms(), '<fid>', lid)

      if (isAS400(this.dbms())) {
        // AS400はODBCのSQLTablesではうまく取得できないのでSQLで取得する
        getAS400Schemas(this).then((schemas) => {
          debugLog('schemas', '<res>', JSON.stringify(schemas), '<fid>', lid)
          resolve(schemas)
        })
      } else if (isMySQL(this.dbms())) {
        // MySQLはスキーマ名がODBCから取得できないためSQLで取得する
        getMySQLSchemas(this).then((schemas) => {
          debugLog('schemas', '<res>', JSON.stringify(schemas), '<fid>', lid)
          resolve(schemas)
        })
      } else {
        // 全て取得するようにする
        condition.schema = '%'
        condition.tableType = '%'
        // テーブル一覧からスキーマのみ抽出する ※重複はカット
        const _tmp = JSON.parse(this._native.tables(condition)).map((item) => `${item.catalog}|${item.schema}`)
        const schemas = [...new Set(_tmp)].map((t) => {
          const [catalog, schema] = t.split('|')
          return {
            catalog: catalog,
            name: schema,
            remarks: '',
          }
        })

        if (isPostgres(this.dbms())) {
          // PostgreSQLはスキーマコメントが設定できるので、SQLで取得する
          setPostgresSchemas(this, schemas).then((s) => {
            debugLog('schemas', '<res>', JSON.stringify(s), '<fid>', lid)
            resolve(s)
          })
        } else {
          debugLog('schemas', '<res>', JSON.stringify(schemas), '<fid>', lid)
          resolve(schemas)
        }
      }
    })
  }
  currentSchema() {
    // カレントスキーマの取得はODBC経由では行えないのでSQLで取得する
    return new Promise((resolve) => {
      const lid = genLogId()

      debugLog('currentSchema', '<db>', this.dbms(), '<fid>', lid)

      if (isAS400(this.dbms())) {
        getAS400CurrentSchema(this).then((schema) => {
          debugLog('currentSchema', '<res>', schema, '<fid>', lid)
          resolve(schema)
        })
      } else if (isMySQL(this.dbms())) {
        getMySQLCurrentSchema(this).then((schema) => {
          debugLog('currentSchema', '<res>', schema, '<fid>', lid)
          resolve(schema)
        })
      } else if (isPostgres(this.dbms())) {
        getPostgresCurrentSchema(this).then((schema) => {
          debugLog('currentSchema', '<res>', schema, '<fid>', lid)
          resolve(schema)
        })
      } else if (isMSSQL(this.dbms())) {
        getMSSQLCurrentSchema(this).then((schema) => {
          debugLog('currentSchema', '<res>', schema, '<fid>', lid)
          resolve(schema)
        })
      } else {
        // その他の場合はカレントスキーマは空文字を返す
        debugLog('currentSchema', '<res>', '', '<fid>', lid)
        resolve('')
      }
    })
  }
  tables(condition) {
    return new Promise((resolve) => {
      const lid = genLogId()
      debugLog('tables', '<cond>', JSON.stringify(condition), '<db>', this.dbms(), '<fid>', lid)

      const resJson = this._native.tables(condition)

      debugLog('tables', '<res #1>', resJson, '<fid>', lid)

      let tables = JSON.parse(resJson)
      if (isPostgres(this.dbms())) {
        // PostgreSQLはテーブル名がODBCから取得できないためSQLで取得する
        setPostgresTables(this, tables).then((t) => {
          debugLog('tables', '<res #2>', JSON.stringify(t), '<fid>', lid)
          resolve(t)
        })
      } else if (isMSSQL(this.dbms())) {
        // SQL Serverはテーブル名がODBCから取得できないためSQLで取得する
        getMSSQLTables(this, tables).then((t) => {
          debugLog('tables', '<res #2>', JSON.stringify(t), '<fid>', lid)
          resolve(t)
        })
      } else {
        if (isMySQL(this.dbms())) {
          tables = getMySQLTables(tables)
        }
        debugLog('tables', '<res #2>', JSON.stringify(tables), '<fid>', lid)
        resolve(tables)
      }
    })
  }
  columns(condition) {
    return new Promise((resolve) => {
      const lid = genLogId()
      debugLog('columns', '<cond>', JSON.stringify(condition), '<db>', this.dbms(), '<fid>', lid)

      const resJson = this._native.columns(condition)

      debugLog('columns', '<res #1>', resJson, '<fid>', lid)

      let columns = JSON.parse(resJson)
      if (isPostgres(this.dbms())) {
        // PostgreSQLはカラムのRemarkがcolumnsからは取得できないので、SQLで取得する
        setPostgresColumns(this, columns).then((c) => {
          debugLog('columns', '<res #2>', JSON.stringify(c), '<fid>', lid)
          resolve(c)
        })
      } else {
        if (isMySQL(this.dbms())) {
          columns = getMySQLColumns(columns)
        }

        debugLog('columns', '<res #2>', JSON.stringify(columns), '<fid>', lid)
        resolve(columns)
      }
    })
  }
  primaryKeys(condition = {}) {
    return new Promise((resolve) => {
      const lid = genLogId()
      debugLog('primaryKeys', '<cond>', JSON.stringify(condition), '<db>', this.dbms(), '<fid>', lid)

      // 主キー情報を取得する
      let keys = JSON.parse(this._native.primaryKeys(condition))
      if (isMySQL(this.dbms())) {
        keys = getMySQLPrimaryKeys(keys)
      }

      debugLog('primaryKeys', '<res>', JSON.stringify(keys), '<fid>', lid)
      resolve(keys)
    })
  }
  query(queryString, options) {
    return new Promise((resolve) => {
      const lid = genLogId()
      debugLog(
        'query',
        '<sql>',
        getLogMsg(queryString),
        '<opt>',
        JSON.stringify(options),
        '<db>',
        this.dbms(),
        '<fid>',
        lid,
      )

      let result = JSON.parse(this._native.query(queryString, options))
      if (isPostgres(this.dbms())) {
        // PostgreSQLはODBCから取得できるカラム情報がおかしいため一部はSQLで取得する
        getPostgresQuery(this, result).then((t) => {
          debugLog('query', '<res #2>', JSON.stringify(t), '<fid>', lid)
          resolve(t)
        })
      } else {
        if (isMySQL(this.dbms())) {
          result = getMySQLQuery(result)
        }

        debugLog('query', '<res>', JSON.stringify(result), '<fid>', lid)
        resolve(result)
      }
    })
  }
  execute(sql) {
    return new Promise((resolve) => {
      const lid = genLogId()
      debugLog('execute', '<sql>', getLogMsg(sql), '<db>', this.dbms(), '<fid>', lid)

      const resJson = this._native.execute(sql)

      debugLog('execute', '<res>', resJson, '<fid>', lid)

      resolve(JSON.parse(resJson))
    })
  }
  records(sql) {
    return new Promise((resolve) => {
      const lid = genLogId()
      debugLog('records', '<sql>', getLogMsg(sql), '<db>', this.dbms(), '<fid>', lid)

      const resJson = this._native.records(sql)
      debugLog('records', '<res>', resJson, '<fid>', lid)

      resolve(JSON.parse(resJson))
    })
  }
  setLocale(category, locale) {
    return this._native.setLocale(category, locale)
  }
}

module.exports = OmniDb
