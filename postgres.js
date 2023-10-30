const { escapeSqlString } = require('./sql.js');

/**
 * PostgreSQLかどうかを判定します。
 * @param {string} dbms DBMS名
 * @returns {boolean} PostgreSQLの場合はtrue
 */
const isPostgres = (dbms) => {
  const regex = /^PostgreSQL/i;
  return regex.test(dbms);
}
exports.isPostgres = isPostgres;

/**
 * PostgreSQLのテーブル情報にテーブルコメントを設定します。
 * @param {object} omnidb omnidbのインスタンス
 * @param {Array} tables テーブル情報配列
 * @returns {Array} テーブル情報配列
 */
const setPostgresTables = async (omnidb, tables) => {
  if(!tables || tables.length == 0) {
    return tables;
  }

  // 検索するテーブル一覧を作成
  const search =
    tables
    .filter((table) => table.schema && table.name)
    .map((table) => "'" + escapeSqlString(`${table.schema}.${table.name}`) + "'");
  if(search.length == 0) {
    return tables;
  }

  // PostgreSQLはテーブルコメントがtablesからは取得できないので、SQLで取得する
  const sql = 
    `SELECT
      *
    FROM
      (
        SELECT
          pg_namespace.nspname || '.' || pg_class.relname AS name,
          pg_namespace.nspname AS schema_name,
          pg_class.relname AS table_name,
          pg_description.description AS remarks
        FROM
          pg_class
          INNER JOIN
            pg_namespace
          ON  pg_class.relnamespace = pg_namespace.oid
          LEFT OUTER JOIN
            pg_description
          ON  pg_class.oid = pg_description.objoid
          AND pg_description.objsubid = 0
        WHERE
          pg_namespace.nspname NOT IN('pg_catalog', 'pg_toast', 'information_schema')
      ) T
    WHERE
      name IN(${search.join(',')})`;

  const res = await omnidb.records(sql);
  const records = res.records;
  const remarkIdx = res.columnIndex.remarks;
  const nameIdx = res.columnIndex.name;
  return tables.map((table) => {
    const row = records.findIndex((rec) => rec[nameIdx] === `${table.schema}.${table.name}`);
    if(row >= 0) {
      // テーブルコメントを設定
      const remarks = records[row][remarkIdx];
      if(remarks && !table.remarks) {
        table.remarks = remarks;
      }
    }
    return table;
  })
}
exports.setPostgresTables = setPostgresTables;

/**
 * PostgreSQLのカラム情報にカラムコメントを設定します。
 * @param {object} omnidb omnidbのインスタンス
 * @param {Array} columns カラム情報配列
 * @returns {Array} カラム情報配列
 */
const setPostgresColumns = async (omnidb, columns) => {
  if(!columns || columns.length == 0) {
    return columns;
  }

  // 検索するテーブル一覧を作成 ※重複はカット
  const search = [...new Set(
      columns
      .filter((column) => column.schema && column.table)
      .map((column) => "'" + escapeSqlString(`${column.schema}.${column.table}`) + "'"))];
  if(search.length == 0) {
    return columns;
  }

  // PostgreSQLはテーブルコメントがcolumnsからは取得できないので、SQLで取得する
  // 後、特定のカラムはSQLColumnsで取得値がおかしい場合があるので、SQLで取得する
  const sql = `
    SELECT
      *
    FROM
      (
        SELECT
          pg_stat_user_tables.schemaname || '.' || pg_stat_user_tables.relname AS name,
          pg_stat_user_tables.schemaname AS schema_name,
          pg_stat_user_tables.relname AS table_name,
          information_schema.columns.column_name AS column_name,
          information_schema.columns.data_type AS data_type,
          pg_description.description AS remarks
        FROM
          pg_stat_user_tables
          INNER JOIN
            information_schema.columns
          ON  pg_stat_user_tables.relname = information_schema.columns.table_name
          LEFT OUTER JOIN
            pg_description
          ON  pg_description.objoid = pg_stat_user_tables.relid
          AND pg_description.objsubid = information_schema.columns.ordinal_position
      ) T
    WHERE
      name IN(${search.join(',')})`;
    
  const res = await omnidb.records(sql);
  const records = res.records;
  const remarkIdx = res.columnIndex.remarks;
  const nameIdx = res.columnIndex.name;
  const dataTypeIdx = res.columnIndex.data_type;
  const columnIndex = res.columnIndex.column_name;
  return columns.map((column) => {
    const row = records.findIndex((rec) => rec[nameIdx] === `${column.schema}.${column.table}` && rec[columnIndex] === column.name);
    if(row >= 0) {
      // カラムコメントを設定
      const remarks = records[row][remarkIdx];
      if(remarks && !column.remarks) {
        column.remarks = remarks;
      }

      const dataType = records[row][dataTypeIdx];
      if (dataType) {
        // 以下のデータ型はODBCから取得した場合VARCHAR(255)等になってしまうので、適切な型に変更する
        switch (dataType.toLowerCase()) {
          case 'array':
          case 'user-defined':  // enum
          case 'json':
          case 'jsonb': {
            // CLOBに変更する
            if (column.type === 'SQL_WVARCHAR') {
              column.type = 'SQL_WLONGVARCHAR'
              column.size = 8190
            } else if (column.type === 'SQL_VARCHAR') {
              column.type = 'SQL_LONGVARCHAR'
              column.size = 8190
            }
            break
          }
          case 'xml': {
            if (column.type === 'SQL_WLONGVARCHAR' || column.type === 'SQL_LONGVARCHAR') {
              column.size = 8190
            }
            break
          }
        }
      }
    }
    return column;
  })
}
exports.setPostgresColumns = setPostgresColumns;

/**
 * PostgreSQLのカレントスキーマを取得する
 * @param {object} omnidb omnidbのインスタンス
 * @returns {string} カレントスキーマ名
 */
const getPostgresCurrentSchema = async (omnidb) => {
  // カレントスキーマを返す
  const sql = `SELECT current_schema()`;
  const res = await omnidb.records(sql);
  return (res?.records?.length > 0) ? res.records[0][0] : '';
}
exports.getPostgresCurrentSchema = getPostgresCurrentSchema;
