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
  setMSSQLTables,
  setMSSQLColumns,
  getMSSQLCurrentSchema,
  getMSSQLQuery,
  filterMSSQLSchemas,
} = require('./mssql.js')

const {
  isOracle,
  setOracleTables,
  filterOracleSchemas,
  getOracleCurrentSchema,
  setOracleColumns,
  getOracleQuery
} = require('./oracle.js')

/**
 * スキーマ検索条件
 * @typedef SchemaCondition
 * @type {Object}
 * @property {string} [catalog] - データベース名。省略した場合は接続中のデータベースが使用されます。%によるワイルドカード指定が可能です。
 * @property {string} [schema] - スキーマ名。省略したい場合は全てのスキーマが対象になります。%によるワイルドカード指定が可能です。
 */

/**
 * テーブル検索条件
 * @typedef TableCondition
 * @type {Object}
 * @property {string} [catalog] - データベース名。省略した場合は接続中のデータベースが使用されます。%によるワイルドカード指定が可能です。
 * @property {string} [schema] - スキーマ名。省略したい場合は全てのスキーマが対象になります。%によるワイルドカード指定が可能です。
 * @property {string} [table] - テーブル名。省略した場合は全てのテーブルが対象になります。%によるワイルドカード指定が可能です。
 * @property {string} [tableType] - テーブルタイプ。省略した場合はテーブルが対象になります。%によるワイルドカード指定が可能です。
 */

/**
 * カラム検索条件
 * @typedef ColumnCondition
 * @type {Object}
 * @property {string} [catalog] - データベース名。省略した場合は接続中のデータベースが使用されます。%によるワイルドカード指定が可能です。
 * @property {string} [schema] - スキーマ名。省略したい場合は全てのスキーマが対象になります。%によるワイルドカード指定が可能です。
 * @property {string} [table] - テーブル名。省略した場合は全てのテーブルが対象になります。%によるワイルドカード指定が可能です。
 * @property {string} [column] - カラム名。省略した場合は全てのカラムが対象になります。%によるワイルドカード指定が可能です。
 */

/**
 * 主キー検索条件
 * @typedef PrimaryKeyCondition
 * @type {Object}
 * @property {string} [catalog] - データベース名。省略した場合は接続中のデータベースが使用されます。%によるワイルドカード指定が可能です。
 * @property {string} [schema] - スキーマ名。省略したい場合は全てのスキーマが対象になります。%によるワイルドカード指定が可能です。
 * @property {string} [table] - テーブル名。省略した場合は全てのテーブルが対象になります。%によるワイルドカード指定が可能です。
 */

/**
 * スキーマ情報
 * @typedef Schema
 * @type {Object}
 * @property {string} catalog - データベース名
 * @property {string} name - スキーマ名
 * @property {string} remarks - スキーマコメント
 */

/**
 * テーブル情報
 * @typedef Table
 * @type {Object}
 * @property {string} catalog - データベース名
 * @property {string} schema - スキーマ名
 * @property {string} name - テーブル名
 * @property {string} type - テーブルタイプ
 * @property {string} remarks - テーブルコメント
 */

/**
 * カラム情報
 * @typedef Column
 * @type {Object}
 * @property {string} catalog - データベース名
 * @property {string} schema - スキーマ名
 * @property {string} table - テーブル名
 * @property {string} name - カラム名
 * @property {string} type - カラムの型名
 * @property {string} typeClass - カラムの型クラス名
 * @property {number} size - カラムのサイズ
 * @property {number} decimalDigits - カラムの小数点以下桁数
 * @property {number} numPrec - カラムの精度
 * @property {string} remarks - カラムコメント
 * @property {string} defualt - カラムのデフォルト値
 * @property {boolean} nullable - カラムがNULL許容かどうか
 */

/**
 * 主キー情報
 * @typedef PrimaryKey
 * @type {Object}
 * @property {string} catalog - データベース名
 * @property {string} schema - スキーマ名
 * @property {string} table - テーブル名
 * @property {string} column - カラム名
 * @property {number} seq - キーのシーケンス番号
 * @property {string} primaryKey - 主キー名
 */

/**
 * OmniDbクラス
 */
class OmniDb {
  /**
   * コンストラクタ
   * @constructor
   */
  constructor() {
    this._native = new OmniDbNative()
    this._dbms = ''
    this._driver = ''
  }

  /**
   * デバッグモードの設定
   * @param {boolean} value デバッグモードの設定値
   */
  static set debug(value) {
    log.isDebug = value
  }

  /**
   * デバッグモードの取得
   * @returns {boolean} デバッグモードの設定値
   */
  static get debug() {
    return log.isDebug
  }

  /**
   * ODBCドライバ一覧を取得します。
   * @returns {Array<string>} ODBCドライバ一覧
   * @async
   */
  drivers() {
    return new Promise((resolve) => {
      const lid = genLogId()
      debugLog('drivers', '<fid>', lid)

      const resJson = this._native.drivers()

      debugLog('drivers', '<res>', resJson, '<fid>', lid)
      resolve(JSON.parse(resJson))
    })
  }

  /**
   * ODBC接続を行います。
   * @param {string} connectionString 接続文字列
   * @returns {boolean} 接続に成功した場合はtrue
   * @throws {Error} 接続に失敗した場合
   * @async
   */
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

  /**
   * ODBC接続を切断します。
   * @returns {boolean} 切断に成功した場合はtrue
   * @throws {Error} 切断に失敗した場合
   * @async
   */
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

  /**
   * 接続中のドライバ名を取得します。
   * @returns {string} ドライバ名
   */
  driver() {
    return this._driver
  }

  /**
   * 接続中のDBMS名を取得します。
   * @returns {string} DBMS名
   */
  dbms() {
    return this._dbms
  }

  /**
   * スキーマ一覧を取得します。
   * @param {SchemaCondition} condition 検索条件
   * @returns {Array<Schema>} スキーマ情報配列
   * @async
   */
  schemas(condition = {}) {
    return new Promise((resolve) => {
      const lid = genLogId()

      debugLog('schemas', '<cond>', JSON.stringify(condition), '<db>', this.dbms(), '<fid>', lid)

      // 現状conditionは未対応

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
        // PostgreSQLはテーブルタイプを%にしないと取得できない。他は空文字にする
        condition.tableType = isPostgres(this.dbms()) ? '%' : ''

        // テーブル一覧からスキーマのみ抽出する ※重複はカット
        const _tmp = JSON.parse(this._native.tables(condition)).map((item) => `${item.catalog}|${item.schema}`)
        let schemas = [...new Set(_tmp)].map((t) => {
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
          if (isMSSQL(this.dbms())) {
            // システムスキーマは除外する
            schemas = filterMSSQLSchemas(schemas)
          } else if (isOracle(this.dbms())) {
            // システムスキーマは除外する
            schemas = filterOracleSchemas(schemas)
          }

          debugLog('schemas', '<res>', JSON.stringify(schemas), '<fid>', lid)
          resolve(schemas)
        }
      }
    })
  }

  /**
   * カレントスキーマを取得します。
   * @returns {string} カレントスキーマ名
   * @async
   */
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
      } else if (isOracle(this.dbms())) {
        getOracleCurrentSchema(this).then((schema) => {
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

  /**
   * テーブル一覧を取得します。
   * @param {TableCondition} condition 検索条件
   * @returns {Array<Table>} テーブル情報配列
   * @async
   */
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
        setMSSQLTables(this, tables).then((t) => {
          debugLog('tables', '<res #2>', JSON.stringify(t), '<fid>', lid)
          resolve(t)
        })
      } else if (isOracle(this.dbms())) {
        // OracleはコメントがODBCから取得できないためSQLで取得する
        setOracleTables(this, tables).then((t) => {
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

  /**
   * カラム一覧を取得します。
   * @param {Condition} condition 検索条件
   * @returns {Array<Column>} カラム情報配列
   * @async
   */
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
      } else if (isMSSQL(this.dbms())) {
        // SQLServerはカラムのRemarkがcolumnsからは取得できないので、SQLで取得する
        setMSSQLColumns(this, columns).then((c) => {
          debugLog('columns', '<res #2>', JSON.stringify(c), '<fid>', lid)
          resolve(c)
        })
      } else if (isOracle(this.dbms())) {
        // OracleはカラムのRemarkがcolumnsからは取得できないので、SQLで取得する
        setOracleColumns(this, columns).then((c) => {
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

  /**
   * 主キー一覧を取得します。
   * @param {PrimaryKeyCondition} condition 検索条件
   * @returns {Array<PrimaryKey>} 主キー情報配列
   * @async
   */
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

  /**
   * SQLの情報を返します
   * @param {string} sql SQL文
   * @param {object} [options] オプション
   * @returns {Array} 実行結果
   * @async
   */
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
      } else if (isMSSQL(this.dbms())) {
        // SQL ServerはODBCから取得できるカラム情報がおかしいため一部はSQLで取得する
        getMSSQLQuery(this, result, queryString).then((t) => {
          debugLog('query', '<res #2>', JSON.stringify(t), '<fid>', lid)
          resolve(t)
        })
      } else if (isOracle(this.dbms())) {
        // OracleはODBCから取得できるカラム情報がおかしいため一部はSQLで取得する
        getOracleQuery(this, result, queryString).then((t) => {
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

  /**
   * SQLを実行します
   * @param {string} sql SQL文
   * @returns {boolean} 実行結果
   * @async
   */
  execute(sql) {
    return new Promise((resolve) => {
      const lid = genLogId()
      debugLog('execute', '<sql>', getLogMsg(sql), '<db>', this.dbms(), '<fid>', lid)

      const resJson = this._native.execute(sql)

      debugLog('execute', '<res>', resJson, '<fid>', lid)

      resolve(JSON.parse(resJson))
    })
  }

  /**
   * SQLを実行しレコードを取得します
   * @param {string} sql SQL文
   * @returns {Array} 実行結果
   * @async
   */
  records(sql) {
    return new Promise((resolve) => {
      const lid = genLogId()
      debugLog('records', '<sql>', getLogMsg(sql), '<db>', this.dbms(), '<fid>', lid)

      const resJson = this._native.records(sql)
      debugLog('records', '<res>', resJson, '<fid>', lid)

      resolve(JSON.parse(resJson))
    })
  }

  /**
   * ロケールを設定します。
   * @param {string} category カテゴリ名
   * @param {string} locale ロケール名
   * @returns {boolean} 成功した場合はtrue
   */
  setLocale(category, locale) {
    return this._native.setLocale(category, locale)
  }
}

module.exports = OmniDb
