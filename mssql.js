const { escapeSqlString } = require('./sql.js')

/**
 * SQL Serverかどうかを判定します。
 * @param {string} dbms DBMS名
 * @returns {boolean} SQL Serverの場合はtrue
 */
const isMSSQL = (dbms) => {
  const regex = /^Microsoft SQL Server/i
  return regex.test(dbms)
}
exports.isMSSQL = isMSSQL

/**
 * FreeTDSのドライバかどうかを判定します。
 * @param {string} driver ドライバ名
 * @returns {boolean} FreeTDSの場合はtrue
 */
const isFreetds = (driver) => {
  // FreeTDSのドライバ名はlibtdsodbcで始まる
  const regex = /^libtdsodbc/i
  return regex.test(driver)
}
exports.isFreetds = isFreetds

/**
 * SQL Serverの接続中のカレントスキーマを取得する
 * @param {object} omnidb omnidbのインスタンス
 * @returns {string} カレントスキーマ名
 */
const getMSSQLCurrentSchema = async (omnidb) => {
  // SQL Serverの規定のスキーマを取得
  const sql = `SELECT SCHEMA_NAME()`
  const res = await omnidb.records(sql)
  return res?.records?.length > 0 ? res.records[0][0] : ''
}
exports.getMSSQLCurrentSchema = getMSSQLCurrentSchema

/**
 * SQLServerのテーブル情報にテーブルコメントを設定します。※FreeTDS(MS版も取得できないはず)
 * @param {object} omnidb omnidbのインスタンス
 * @param {Array} tables テーブル情報配列
 * @returns {Array} テーブル情報配列
 */
const setMSSQLTables = async (omnidb, tables) => {
  if (!tables || tables.length == 0) {
    return tables
  }

  // 検索するテーブル一覧を作成
  const search = tables
    .filter((table) => table.schema && table.name)
    .map((table) => "'" + escapeSqlString(`${table.schema}.${table.name}`) + "'")
  if (search.length == 0) {
    return tables
  }

  // SQLServerはテーブルコメントがtablesからは取得できないので、SQLで取得する
  const sql = `SELECT
      *
    FROM (
      SELECT 
        t.name AS TableName,
        SCHEMA_NAME(t.schema_id) AS SchemaName,
        CONCAT(t.name, '.', SCHEMA_NAME(t.schema_id)) AS name,
        ep.value AS remarks
      FROM 
        sys.tables t
      LEFT JOIN 
        sys.extended_properties ep ON
          t.object_id = ep.major_id
          AND ep.minor_id = 0
          AND ep.name = 'MS_Description'
    ) BASE
    WHERE
    name IN(${search.join(',')})`

  const res = await omnidb.records(sql)
  const records = res.records
  const remarkIdx = res.columnIndex.remarks
  const nameIdx = res.columnIndex.name
  return tables.map((table) => {
    const row = records.findIndex((rec) => rec[nameIdx] === `${table.schema}.${table.name}`)
    if (row >= 0) {
      // テーブルコメントを設定
      const remarks = records[row][remarkIdx]
      if (remarks && !table.remarks) {
        table.remarks = remarks
      }
    }
    return table
  })
}
exports.setMSSQLTables = setMSSQLTables
