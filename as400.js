const { trimSpaces, escapeSqlString } = require('./sql')
const { debugLog } = require('./log')

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
 * カラムの型を変換する（GRAPHIC→SQL_WCHAR、VARG→SQL_WVARCHAR）
 * @param {object} column カラム情報
 * @param {string} dataType DATA_TYPE
 * @param {number} ccsid CCSID
 * @returns {object} 変換後のカラム情報
 */
const transformAS400ColumnType = (column, dataType, ccsid) => {
  // 元のカラムがSQL_CHAR、SQL_VARCHAR、SQL_LONGVARCHARの場合
  if (['SQL_CHAR', 'SQL_VARCHAR', 'SQL_LONGVARCHAR'].includes(column.type)) {
    if (ccsid === 16684) {
      // UCS-2
      if (dataType === 'GRAPHIC') {
        return { ...column, type: 'SQL_WCHAR' }
      } else if (dataType === 'VARG') {
        return { ...column, type: 'SQL_WVARCHAR' }
      }
    }
  }
  return column
}

/**
 * テーブルごとのカラム情報を取得する
 * @param {object} omnidb omnidbのインスタンス
 * @param {Array<{schema: string, table: string}>} tables テーブル一覧
 * @returns {Map} テーブルごとのカラム情報マップ
 */
const getAS400ColumnInfoMap = async (omnidb, tables) => {
  const tableColumnInfoMap = new Map()

  for (const table of tables) {
    const sql = `
      SELECT
        COLUMN_NAME,
        DATA_TYPE,
        CCSID,
        -- カラムコメントはCOLUMN_TEXT、LONG_COMMENT、COLUMN_HEADINGのいずれかを使用する※左が優先
        COALESCE(
          NULLIF(TRIM(COLUMN_TEXT), ''),
          NULLIF(TRIM(LONG_COMMENT), ''),
          NULLIF(TRIM(COLUMN_HEADING), '')
        ) AS REMARKS
      FROM
        QSYS2.SYSCOLUMNS2
      WHERE
        TABLE_SCHEMA = '${escapeSqlString(table.schema)}'
        AND TABLE_NAME = '${escapeSqlString(table.table)}'
      ORDER BY
        ORDINAL_POSITION`

    const res = await omnidb.records(sql)
    const columnInfoMap = new Map()

    res.records.forEach((rec) => {
      const columnName = trimSpaces(rec[res.columnIndex.COLUMN_NAME])
      columnInfoMap.set(columnName, {
        dataType: trimSpaces(rec[res.columnIndex.DATA_TYPE]),
        ccsid: rec[res.columnIndex.CCSID],
        remarks: rec[res.columnIndex.REMARKS] ? trimSpaces(rec[res.columnIndex.REMARKS]) : null,
      })
    })

    tableColumnInfoMap.set(`${table.schema}.${table.table}`, columnInfoMap)
  }

  return tableColumnInfoMap
}

/**
 * AS400のカラム情報にGRAPHIC/VARG型のカラムをWCHAR/WVARCHARに変換します。
 * @param {object} omnidb omnidbのインスタンス
 * @param {Array} columns カラム情報配列
 * @returns {Array} カラム情報配列
 */
const setAS400Columns = async (omnidb, columns) => {
  if (!columns || columns.length == 0) {
    return columns
  }

  try {
    // スキーマとテーブル情報を持つカラムをフィルタ
    const columnsWithTable = columns.filter((col) => col.schema && col.table)
    if (columnsWithTable.length === 0) {
      return columns
    }

    // 重複を除いたテーブル一覧を作成
    const tables = [...new Set(columnsWithTable.map((col) => `${col.schema}.${col.table}`))].map((tableStr) => {
      const [schema, table] = tableStr.split('.')
      return { schema, table }
    })

    // テーブルごとのカラム情報を取得
    const tableColumnInfoMap = await getAS400ColumnInfoMap(omnidb, tables)

    // カラム情報を変換
    return columns.map((column) => {
      if (!column.schema || !column.table) {
        return column
      }

      const columnInfoMap = tableColumnInfoMap.get(`${column.schema}.${column.table}`)
      if (!columnInfoMap) {
        return column
      }

      const columnInfo = columnInfoMap.get(column.name)
      if (!columnInfo) {
        return column
      }

      // カラムコメントを設定
      let updatedColumn = column

      if (columnInfo.remarks && !column.remarks) {
        updatedColumn = { ...column, remarks: columnInfo.remarks }
      }

      // 型を変換
      return transformAS400ColumnType(updatedColumn, columnInfo.dataType, columnInfo.ccsid)
    })
  } catch (e) {
    // エラーが発生した場合は元のカラムをそのまま返す
    debugLog('setAS400Columns error', e)
    return columns
  }
}
exports.setAS400Columns = setAS400Columns

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

/**
 * AS400のクエリ結果のカラム型を変換する
 * @param {OmniDb} omnidb omnidbのインスタンス
 * @param {Object} result クエリ結果
 * @param {string} sql 実行したSQL
 * @returns {Object} AS400のクエリ結果
 */
const getAS400Query = async (omnidb, result, sql) => {
  if (!result?.columns?.length > 0) {
    // データがなければそのまま
    return result
  }

  try {
    // クエリ結果のカラムに対してスキーマとテーブル情報が取得できる場合のみ処理
    const columnsWithTable = result.columns.filter((col) => col.schema && col.table)

    if (columnsWithTable.length === 0) {
      // スキーマ・テーブル情報がない場合はそのまま返す
      return result
    }

    // 重複を除いたテーブル一覧を作成
    const tables = [...new Set(columnsWithTable.map((col) => `${col.schema}.${col.table}`))].map((tableStr) => {
      const [schema, table] = tableStr.split('.')
      return { schema, table }
    })

    // テーブルごとのカラム情報を取得
    const tableColumnInfoMap = await getAS400ColumnInfoMap(omnidb, tables)

    // カラム情報を変換
    const columns = result.columns.map((column) => {
      if (!column.schema || !column.table) {
        return column
      }

      const columnInfoMap = tableColumnInfoMap.get(`${column.schema}.${column.table}`)
      if (!columnInfoMap) {
        return column
      }

      // QueryColumnの場合は元のカラム名を使用、Columnの場合はnameを使用
      const columnName = column.column || column.name
      const columnInfo = columnInfoMap.get(columnName)
      if (!columnInfo) {
        return column
      }

      // 型を変換（QueryColumnの場合はremarksは設定しない）
      return transformAS400ColumnType(column, columnInfo.dataType, columnInfo.ccsid)
    })

    return {
      ...result,
      columns,
    }
  } catch (e) {
    // エラーが発生した場合は元の結果をそのまま返す
    debugLog('getAS400Query error', e)
    return result
  }
}
exports.getAS400Query = getAS400Query
