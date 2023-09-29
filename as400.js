/**
 * AS400かどうかを判定します。
 * @param {string} dbms DBMS名
 * @returns {boolean} AS400の場合はtrue
 */
const isAS400 = (dbms) => {
  const regex = /^DB2\/400/i;
  return regex.test(dbms);
}
exports.isAS400 = isAS400;

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
      SCHEMA_NAME`;
  const res = await omnidb.records(sql);
  const records = res.records;
  const nameIdx = res.columnIndex.SCHEMA_NAME;
  const remarksIdx = res.columnIndex.SCHEMA_TEXT;

  // スキーマとして返却
  return records.map((rec) => {
    return {
      // catalogは空にする
      catalog: '',
      name: rec[nameIdx],
      remarks: rec[remarksIdx] || '',
    }
  });
}
exports.getAS400Schemas = getAS400Schemas;

