const { trimSpaces, escapeSqlString } = require('./sql')

/**
 * AS400かどうかを判定します。
 * @param {string} dbms DBMS名
 * @returns {boolean} AS400の場合はtrue
 */
const isAS400 = (dbms) => {
  const regex = /^DB2\/400/i
  return regex.test(dbms)
}
exports.isAS400 = isAS400

/**
 * AS400のスキーマ名を取得する
 * @param {OmniDb} omnidb omnidbのインスタンス
 * @returns {Array<object>} スキーマの配列
 */
const getAS400Schemas = async (omnidb) => {
  // AS400はODBCのSQLTablesではうまく取得できないのでSQLで取得する

  // スキーマ名を取得する
  const sql = `
    SELECT
      SCHEMA_NAME,
      SCHEMA_TEXT
    FROM
      QSYS2.SYSSCHEMAS
    ORDER BY
      SCHEMA_NAME`
  const res = await omnidb.records(sql)
  const records = res.records
  const nameIdx = res.columnIndex.SCHEMA_NAME
  const remarksIdx = res.columnIndex.SCHEMA_TEXT

  // スキーマとして返却
  const schemas = records.map((rec) => {
    return {
      // catalogは空にする
      catalog: '',
      name: trimSpaces(rec[nameIdx]),
      remarks: trimSpaces(rec[remarksIdx]) || '',
    }
  })
  return schemas
}
exports.getAS400Schemas = getAS400Schemas

/**
 * AS400の接続中のカレントスキーマを取得する
 * @param {OmniDb} omnidb omnidbのインスタンス
 * @returns {string} カレントスキーマ名
 */
const getAS400CurrentSchema = async (omnidb) => {
  // カレントスキーマを返す
  const sql = `VALUES CURRENT SCHEMA`
  const res = await omnidb.records(sql)
  return res?.records?.length > 0 ? trimSpaces(res.records[0][0]) : ''
}
exports.getAS400CurrentSchema = getAS400CurrentSchema

/**
 * AS400のテーブル情報にテーブルコメントを設定します。
 * @param {object} omnidb omnidbのインスタンス
 * @param {Array} tables テーブル情報配列
 * @returns {Array} テーブル情報配列
 */
const setAS400Tables = async (omnidb, tables) => {
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

  // 検索対象のスキーマ一覧を作成
  const schemas = [...new Set(tables.map((table) => "'" + escapeSqlString(table.schema) + "'"))]
  if (schemas.length == 0) {
    return tables
  }

  // 検索対象のテーブル一覧を作成
  const tableNames = [...new Set(tables.map((table) => "'" + escapeSqlString(table.name) + "'"))]
  if (tableNames.length == 0) {
    return tables
  }

  // AS400はテーブルコメントがtablesからは取得できないので、SQLで取得する
  // ※データベース内の全オブジェクトが対象のため一旦絞る
  const sql = `
    SELECT
      *
    FROM
      (
        SELECT
          CAST(TRIM(TABLE_SCHEMA) || '.' || TRIM(TABLE_NAME) AS VARCHAR(512)) AS AB_NAME,
          TABLE_SCHEMA,
          TABLE_NAME,
          TABLE_TEXT AS REMARKS
        FROM
          QSYS2.SYSTABLES
        WHERE
          FILE_TYPE <> 'S'
          AND TABLE_SCHEMA IN (${schemas.join(',')})
          AND TABLE_NAME IN (${tableNames.join(',')})
      ) T
    WHERE
      AB_NAME IN (${search.join(',')})`

  const res = await omnidb.records(sql)
  const records = res.records
  const remarkIdx = res.columnIndex.REMARKS
  const nameIdx = res.columnIndex.AB_NAME
  return tables.map((table) => {
    const row = records.findIndex((rec) => rec[nameIdx] === `${table.schema}.${table.name}`)
    if (row >= 0) {
      // テーブルコメントを設定
      const remarks = records[row][remarkIdx]
      if (remarks && !table.remarks) {
        table.remarks = trimSpaces(remarks)
      }
    }
    return table
  })
}
exports.setAS400Tables = setAS400Tables
