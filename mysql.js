/**
 * MySQL/MariaDBかどうかを判定します。
 * @param {string} dbms DBMS名
 * @returns {boolean} MySQL/MariaDbの場合はtrue
 */
const isMySQL = (dbms) => {
  const regex = /^(MySQL|MariaDB)/i;
  return regex.test(dbms);
}
exports.isMySQL = isMySQL;

/**
 * MySQLのスキーマ名を取得する
 * @param {OmniDb} omnidb omnidbのインスタンス
 * @returns {Array<object>} スキーマの配列
 */
const getMySQLSchemas = async (omnidb) => {
  // MySQLはODBCのSQLTablesではうまく取得できないのでSQLで取得する

  // システム以外のスキーマ名を取得する
  const sql = `
    SELECT
      schema_name
    FROM
      information_schema.schemata
    WHERE
      schema_name NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
    ORDER BY
      schema_name`;
  const res = await omnidb.records(sql);
  const records = res.records;
  const nameIdx = res.columnIndex.SCHEMA_NAME;

  // スキーマとして返却
  return records.map((rec) => {
    return {
      // MySQLはcatalog、schemaのどちらにもスキーマ名を設定する
      catalog: rec[nameIdx],
      name: rec[nameIdx],
      remarks: ''
    }
  });
}
exports.getMySQLSchemas = getMySQLSchemas;

/**
 * MySQLのテーブル情報を取得する
 * @param {Array} tables テーブル情報
 * @returns {Array} MySQLのテーブル情報
 */
const getMySQLTables = (tables) => {
  // MySQLはDB名とスキーマ名のどちらしか扱えないため、入っている方をコピーする
  // ※APIによっては設定しても無視されことがあるので、両方設定する
  return tables.map((table) => {
    const name = table.catalog || table.schema;
    return {
      ...table,
      catalog: name,
      schema: name,
    }
  })
}
exports.getMySQLTables = getMySQLTables;

/**
 * MySQLのカラム情報を取得する
 * @param {Array} columns カラム情報
 * @returns {Array} MySQLのカラム情報
 */
const getMySQLColumns = (columns) => {
  // MySQLはDB名とスキーマ名のどちらしか扱えないため、入っている方をコピーする
  // ※APIによっては設定しても無視されことがあるので、両方設定する
  return columns.map((column) => {
    const name = column.catalog || column.schema;
    return {
      ...column,
      catalog: name,
      schema: name,
    }
  })
}
exports.getMySQLColumns = getMySQLColumns;

/**
 * MySQLの主キー情報を取得する
 * @param {Array} keys 主キー情報
 * @returns {Array} MySQLの主キー情報
 */
const getMySQLPrimaryKeys = (keys) => {
  // MySQLはDB名とスキーマ名のどちらしか扱えないため、入っている方をコピーする
  // ※APIによっては設定しても無視されことがあるので、両方設定する
  return keys.map((key) => {
    const name = key.catalog || key.schema;
    return {
      ...key,
      catalog: name,
      schema: name,
    }
  })
}
exports.getMySQLPrimaryKeys = getMySQLPrimaryKeys;

/**
 * MySQLのクエリ結果を取得する
 * @param {Object} result クエリ結果
 * @returns {Object} MySQLのクエリ結果
 */
const getMySQLQuery = (result) => {
  // MySQLはDB名とスキーマ名のどちらしか扱えないため、入っている方をコピーする
  // ※APIによっては設定しても無視されことがあるので、両方設定する
  const columns = result.columns.map((column) => {
    const name = column.catalog || column.schema;
    return {
      ...column,
      catalog: name,
      schema: name,
    }
  })
  const res = {
    ...result,
  }
  res.columns = columns;
  return res;
}
exports.getMySQLQuery = getMySQLQuery;

/**
 * MySQLの接続中のカレントスキーマを取得する
 * @param {object} omnidb omnidbのインスタンス
 * @returns {string} カレントスキーマ名
 */
const getMySQLCurrentSchema = async (omnidb) => {
  // MySQLはスキーマー＝データベースなのでカレントデータベースを返す
  const sql = `SELECT DATABASE()`;
  const res = await omnidb.records(sql);
  return (res?.records?.length > 0) ?res.records[0][0] : '';
}
exports.getMySQLCurrentSchema = getMySQLCurrentSchema;
