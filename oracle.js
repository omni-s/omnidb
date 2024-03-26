const OmniDb = require('./omnidb.js')
const { escapeSqlString, replaceSpecialChars } = require('./sql.js')
const { debugLog } = require('./log.js')
const short = require('short-uuid')

// 短いUUIDの作成
const translator = short()

/**
 * SQL文字列のエスケープ
 * @param {string} s SQL文字列
 * @returns {string} エスケープしたSQL文字列
 */
const getEscString = (s) => {
  // エスケープシーケンスを置換＋シングルクオートを２つに変換
  return replaceSpecialChars(escapeSqlString(s, false))
}

/**
 * Oracleのプレースホルダを変換します
 * @param {string} sql SQL
 * @returns {string} 変換されたSQL
 */
const convertPlaceHolder = (sql) => {
  let paramIndex = 1
  // Oracleが直接理解できるプレースホルダに変換する。:から始まる
  // 例:
  // SELECT * FROM table WHERE id = ? AND name = ?
  // ↓
  // SELECT * FROM table WHERE id = :P1 AND name = :P2
  return sql.replace(/\?+/g, () => {
    return `:P${paramIndex++}`
  })
}

/**
 * 実行計画からSQL情報取得
 * @param {OmniDb} db omnidb
 * @param {string} query 調査したいクエリ
 * @returns {string} ステートメントID
 */
const getPlanInfo = async (db, query) => {
  // ステートメントIDの作成(30文字以内に収まる短いuuid)
  //
  // EXPLAIN PLANの実行には
  // GRANT ALTER SESSION TO ユーザー名;
  // PLAN_TABLEのアクセスには
  // GRANT SELECT ON SYS.PLAN_TABLE TO ユーザー名;
  // が必要です。PLAN_TABLEに関しては一般的に付与されている事が多いです
  const st = translator.new()
  const plan = `
    DECLARE
      q VARCHAR2(8000) := '${query}';
    BEGIN
      -- 実行計画をID付きで実行
      EXECUTE IMMEDIATE 'EXPLAIN PLAN SET STATEMENT_ID = ''${st}'' FOR ' || q;
    END;`
  await db.records(plan)
  return st
}

/**
 * カラム推測値の設定
 * @param {OmniDb} db omnidb
 * @param {string} st ステートメントID
 * @param {object} queryInfo query結果オブジェクト
 */
const setGuessedColumn = async (db, st, queryInfo) => {
  const res = await db.records(`
    SELECT
      OBJECT_OWNER,
      OBJECT_NAME,
      PROJECTION
    FROM
      PLAN_TABLE
    WHERE
      STATEMENT_ID = '${st}'
    ORDER BY
      /* オブジェクトの位置に対応する順序(左から右) */
      OBJECT_INSTANCE`)

  res.records.forEach((row) => {
    const schema = row[0]
    const table = row[1]
    // projectionをカラム一覧を取得
    const columns = getPlanColumns(row[2])

    // カラム情報に推測値を設定
    queryInfo.columns.forEach((column) => {
      if (column.schema || column.table || column.guessedSchema || column.guessedTable) {
        // スキーマ、テーブル、推測値がどれも設定されていない場合だけ設定
        return
      }
  
      const idx = columns.findIndex((item) => item === column.column)
      if (idx >= 0) {
        // カラム名が同じ場合は推測値のスキーマ、テーブル名として設定
        column.guessedSchema = schema
        column.guessedTable = table
      }
    })
  })
}

/**
 * プラン情報の削除
 * @param {OmniDb} db omnidb
 * @param {string} st ステートメントID
 */
const clearPlanInfo = async (db, st) => {
  try {
    // 使ったPLANの情報は削除
    await db.records(`
      DELETE FROM
        PLAN_TABLE
      WHERE
        STATEMENT_ID = '${st}'`)
  } catch (e) {
    // 例外は無視
    debugLog('clearPlanInfo Error:', e)
  }
}

/**
 * PLANのProjectionからカラム一覧を取得する
 * @param {string} projection planのprojection文字列
 * @returns {Array<string>} カラム一覧
 */
const getPlanColumns = (projection) => {
  // ()や[]で囲まれた文字列を削除
 const items = projection.replace(/\(.*?\)/g, '').replace(/\[.*?\]/g, '').split(',').map(i => i.trim())
 return items.map(item => {
   // "A"."B"、A.B、"B"、Bの形式からBを抽出
   const res = item.split('.').map(i => i.replace(/^"|"$/g, ''))
   return res.length > 1 ? res[1] : res[0]
 })
}


/**
 * MySQL/MariaDBかどうかを判定します。
 * @param {string} dbms DBMS名
 * @returns {boolean} MySQL/MariaDbの場合はtrue
 */
const isOracle = (dbms) => {
  const regex = /^(Oracle)/i
  return regex.test(dbms)
}
exports.isOracle = isOracle

/**
 * Oracleのテーブル情報にテーブルコメントを設定します。
 * @param {object} omnidb omnidbのインスタンス
 * @param {Array} tables テーブル情報配列
 * @returns {Array} テーブル情報配列
 */
const setOracleTables = async (omnidb, tables) => {
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
  const schemas = [ ...new Set(tables.map((table) => "'" + escapeSqlString(table.schema) + "'")) ]
  if (schemas.length == 0) {
      return tables
  }

  // 検索対象のテーブル一覧を作成
  const tableNames = [ ...new Set(tables.map((table) => "'" + escapeSqlString(table.name) + "'")) ]
  if (tableNames.length == 0) {
      return tables
  }

  // Oracleはテーブルコメントがtablesからは取得できないので、SQLで取得する
  // ※データベース内の全オブジェクトが対象のため一旦絞る
  const sql = `
    SELECT
      *
    FROM
      (
        SELECT
          OWNER || '.' || TABLE_NAME AS NAME,
          OWNER AS SCHEMA_NAME,
          TABLE_NAME,
          COMMENTS AS REMARKS
        FROM
          ALL_TAB_COMMENTS
        WHERE
          OWNER IN (${schemas.join(',')})
          AND TABLE_NAME IN (${tableNames.join(',')})
      ) T
    WHERE
      NAME IN (${search.join(',')})`

  const res = await omnidb.records(sql)
  const records = res.records
  const remarkIdx = res.columnIndex.REMARKS
  const nameIdx = res.columnIndex.NAME
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
exports.setOracleTables = setOracleTables

/**
 * Oracleのカラム情報にカラムコメントを設定します。
 * @param {object} omnidb omnidbのインスタンス
 * @param {Array} columns カラム情報配列
 * @returns {Array} カラム情報配列
 */
const setOracleColumns = async (omnidb, columns) => {
  if (!columns || columns.length == 0) {
    return columns
  }

  // 検索するテーブル一覧を作成 ※重複はカット
  const search = [
    ...new Set(
      columns
        .filter((column) => column.schema && column.table)
        .map((column) => "'" + escapeSqlString(`${column.schema}.${column.table}`) + "'"),
    ),
  ]
  if (search.length == 0) {
    return columns
  }

  // 検索対象のスキーマ一覧を作成
  const schemas = [ ...new Set(columns.map((column) => "'" + escapeSqlString(column.schema) + "'")) ]
  if (schemas.length == 0) {
      return columns
  }

  // 検索対象のテーブル一覧を作成
  const tableNames = [ ...new Set(columns.map((column) => "'" + escapeSqlString(column.table) + "'")) ]
  if (tableNames.length == 0) {
      return columns
  }

  // Oracleはテーブルコメントがcolumnsからは取得できないので、SQLで取得する
  const sql = `
    SELECT
      *
    FROM
      (
        SELECT
          OWNER || '.' || TABLE_NAME AS NAME,
          OWNER AS SCHEMA_NAME,
          TABLE_NAME,
          COLUMN_NAME,
          COMMENTS AS REMARKS
        FROM
          ALL_COL_COMMENTS
        WHERE
          OWNER IN (${schemas.join(',')})
          AND TABLE_NAME IN (${tableNames.join(',')})
      ) T
    WHERE
      NAME IN (${search.join(',')})`
  const res = await omnidb.records(sql)
  const records = res.records
  const remarkIdx = res.columnIndex.REMARKS
  const nameIdx = res.columnIndex.NAME
  const columnIndex = res.columnIndex.COLUMN_NAME
  return columns.map((column) => {
    const row = records.findIndex(
      (rec) => rec[nameIdx] === `${column.schema}.${column.table}` && rec[columnIndex] === column.name,
    )
    if (row >= 0) {
      // カラムコメントを設定
      const remarks = records[row][remarkIdx]
      if (remarks && !column.remarks) {
        column.remarks = remarks
      }
    }
    return column
  })
}
exports.setOracleColumns = setOracleColumns


/**
 * Oracleのクエリ結果を取得する
 * @param {OmniDb} omnidb omnidbのインスタンス
 * @param {Object} result クエリ結果
 * @param {string} sql 実行したSQL
 * @returns {Object} Oracleのクエリ結果
 */
const getOracleQuery = async (omnidb, result, sql) => {
  if (!result?.columns?.length > 0) {
    // データがなければそのまま
    return result
  }

  const queryInfo = {
    ...result
  }

  let st = ''
  try {
    // Oracleの場合取得元のデータベース、スキーマ、テーブルが取得できない
    // そのため実行計画から推測値としてスキーマ、テーブル名を設定する

    // プレースホルダをOracleのネイティブなプレースホルダに変換
    // ※SQLの文字列として整形
    const _sql = getEscString(convertPlaceHolder(sql))

    //
    // 実行計画でSQL情報取得
    //
    st = await getPlanInfo(omnidb, _sql)

    // 推測値を設定
    await setGuessedColumn(omnidb, st, queryInfo)
  } catch (e) {
    // 補助のためクエリが発生した場合はエラーを無視
    debugLog('plan error', e)
  } finally {
    if (st) {
      // プランを設定している場合はクリア
      clearPlanInfo(omnidb, st)
    }
  }

  return queryInfo
}
exports.getOracleQuery = getOracleQuery


/**
 * Oracleのカレントスキーマを取得する
 * @param {object} omnidb omnidbのインスタンス
 * @returns {string} カレントスキーマ名
 */
const getOracleCurrentSchema = async (omnidb) => {
    // カレントスキーマを返す
    const sql = `SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL`
    const res = await omnidb.records(sql)
    return res?.records?.length > 0 ? res.records[0][0] : ''
}
exports.getOracleCurrentSchema = getOracleCurrentSchema


/**
 * システムスキーマを除いた一覧を返却する
 * @param {array<object>} schemas - スキーマ一覧
 * @returns {array<object>} システムスキーマを除いたスキーマ一覧
 */
const filterOracleSchemas = async (schemas) => {
  // システムで作成されたアカウント（スキーマ）を除外する
  //
  // 参考:
  // https://docs.oracle.com/cd/E82638_01/axdbi/oracle-database-system-privileges-accounts-and-passwords.html#GUID-7513171C-1055-48BB-8C79-B27EECC9B7E9
  // ※上記以外もあるので追加しています
  const systemAccounts = [
    // HTTPによるOracle XML DBへのアクセスを有効化。
    'ANONYMOUS',
    // Oracle Application Expressスキーマおよびメタデータを所有するアカウント。
    'APEX_050000',
    // Oracle Application ExpressリスナーまたはOracle HTTP Serverおよびmod_plsqlを使用したOracle Application Expressの構成に使用する最小限の権限が付与されたアカウント。
    'APEX_PUBLIC_USER',
    // Oracle Quality of Service Managementで必要なすべてのデータおよびメタデータの格納および管理に使用されます。
    'APPQOSSYS',
    // 統合された監査証跡データが存在するアカウント。
    'AUDSYS',
    // Oracle Textアカウント。
    'CTXSYS',
    // DBMS_SFW_ACL_ADMINパッケージの実行に使用されるアカウント。
    'DBSFWUSER',
    // Oracle Enterprise Managerの管理エージェント・コンポーネントによりデータベースの監視および管理に使用されるアカウント。
    'DBSNMP',
    // Directory Integration Platform(DIP)でOracle Internet Directoryでの変更をデータベース内のアプリケーションと同期化するために使用されるアカウント。
    'DIP',
    // Database Vaultが所有するアカウントで、Database Vaultのファクタ値を取得するためのパブリック・ファンクションが含まれます。
    'DVF',
    // このアカウントには2つのロールが関連付けられています。Database Vault所有者ロールは、Database Vaultロールおよび構成を管理します。Database Vaultアカウント・マネージャは、データベース・ユーザー・アカウントの管理に使用されます。
    'DVSYS',
    // Oracle Application Expressのアップロードされたファイルを所有するアカウント。
    'FLOWS_FILES',
    // Oracle GoldenGateが使用する内部アカウント。ロック解除したり、データベース・ログインに使用したりしないでください。
    'GGSYS',
    // Global Data Servicesスキーマを所有する内部アカウント。ロック解除したり、データベース・ログインに使用したりしないでください。
    'GSMADMIN_INTERNAL',
    // グローバル・サービス・マネージャがGlobal Data Servicesカタログへの接続に使用するアカウント。
    'GSMCATUSER',
    // グローバル・サービス・マネージャがデータベースへの接続に使用するアカウント。
    'GSMUSER',
    // Oracleサンプル・スキーマに含まれるHuman Resourcesスキーマを所有するアカウント。
    'HR',
    // Oracle Label Securityの管理者アカウント。
    'LBACSYS',
    // Oracle Spatial and Graphでジオコーダおよびルーター・データの格納に使用されるスキーマ。
    'MDDATA',
    // Oracle Spatial and GraphおよびOracle Multimedia Locatorの管理者アカウント。
    'MDSYS',
    // 
    'OJVMSYS',
    // 
    'OLAPSYS',
    // このアカウントには、Oracle Configuration Managerで使用される構成収集向けのインスツルメンテーションが含まれます。
    'ORACLE_OCM',
    // このアカウントには、Oracle Multimedia DICOMデータ・モデルが含まれます。
    'ORDDATA',
    // Oracle Multimediaユーザー。Oracleにより提供されるプラグインおよびサード・パーティのプラグインはこのスキーマにインストールされます。
    'ORDPLUGINS',
    // Oracle Multimedia管理者アカウント。
    'ORDSYS',
    // プラン・スタビリティをサポートするアカウント。プラン・スタビリティは、同じSQL文の同じ実行計画の保守を可能にします。OUTLNは、格納されたアウトラインに関連付けられたメタデータを集中的に管理するロールとして機能します。
    'OUTLN',
    // データベースのリモート・ジョブを無効化するアカウント。このアカウントはリモート・スケジューラ・エージェントの構成時に作成されます。データベースでリモート・ジョブを実行する機能は、このユーザーを削除することで、無効(使用禁止)にできます。
    'REMOTE_SCHEDULER_AGENT',
    // SQL/MM Still Image Standardの情報ビューを格納するアカウント。
    'SI_INFORMTN_SCHEMA',
    // Catalog Services for the Web(CSW)アカウント。データベースからすべてのレコード・タイプ・メタデータとレコード・インスタンスを、キャッシュされたレコード・タイプのメイン・メモリーにロードするために、Oracle Spatial and Graph CSWキャッシュ・マネージャで使用されます。
    'SPATIAL_CSW_ADMIN_USR',
    // Web Feature Service(WFS)アカウント。データベースからすべてのフィーチャ・タイプ・メタデータとすべてのフィーチャ・インスタンスを、キャッシュされたフィーチャ・タイプのメイン・メモリーにロードするために、Oracle Spatial and Graph WFSキャッシュ・マネージャで使用されます
    'SPATIAL_WFS_ADMIN_USR',
    // データベース管理タスクの実行に使用されるアカウント。
    'SYS',
    // リモートの自動ワークロード・リポジトリ(AWR)などのリモート管理フレームワークの管理に使用されるアカウント。
    'SYS$UMF',
    // バックアップ・タスクおよびリカバリ・タスクの実行に使用されるアカウント。
    'SYSBACKUP',
    // Oracle Data Guardの管理と監視に使用されるアカウント。
    'SYSDG',
    // 暗号化キーの管理の実行に使用されるアカウント。
    'SYSKM',
    // Oracle Real Application Clusters (RAC)の管理に使用されるアカウント。
    'SYSRAC',
    // データベース管理タスクの実行に使用される別のアカウント。
    'SYSTEM',
    // Oracle Workspace Manager用のメタデータ情報の格納に使用されるアカウント。
    'WMSYS',
    // Oracle XML DBのデータおよびメタデータの格納に使用されるアカウント。
    'XDB',
    // セッション内にユーザーが存在しないことを表す内部アカウント。XS$NULLは、ユーザーではないため、Oracle Databaseインスタンスによってのみアクセスできます。XS$NULLには権限がなく、XS$NULLとして認証したり、XS$NULLに認証資格証明を割り当てることはできません。
    'XS$NULL'
  ]

  return schemas.filter((schema) => !systemAccounts.includes(schema.name))
}
exports.filterOracleSchemas = filterOracleSchemas
