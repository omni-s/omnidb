/**
 * スキーマ検索条件
 */
export interface SchemaCondition {
  catalog?: string
  schema?: string
}

/**
 * テーブル検索条件
 */
export interface TableCondition {
  catalog?: string
  schema?: string
  table?: string
  tableType?: string
}

/**
 * カラム検索条件
 */
export interface ColumnCondition {
  catalog?: string
  schema?: string
  table?: string
  column?: string
}

/**
 * 主キー検索条件
 */
export interface PrimaryKeyCondition {
  catalog?: string
  schema?: string
  table?: string
}

/**
 * スキーマ情報
 */
export interface Schema {
  catalog: string
  name: string
  remarks: string
}

/**
 * テーブル情報
 */
export interface Table {
  catalog: string
  schema: string
  name: string
  type: string
  remarks: string
}

/**
 * カラム情報
 */
export interface Column {
  catalog: string
  schema: string
  table: string
  name: string
  type: string
  typeClass: string
  size: number
  decimalDigits: number
  numPrec: number
  remarks: string
  default?: string
  nullable: boolean
}

/**
 * queryのカラム情報
 */
export interface QueryColumn {
  name: string
  label?: string
  type: string
  typeClass: 'String'
  nullable: boolean
  autoIncliment: boolean
  size: number
  octetLength: number
  decimalDigits: number
  catalog: string
  schema: string
  table: string
  column: string
}

/**
 * queryのパラメータ情報
 */
export interface QueryParam {
  decimalDigits: number
  nullable: boolean
  size: number
  type: string
  typeClass: string
}

/**
 * queryのオプション
 */
export interface QueryOptions {
  label?: boolean
}

/**
 * queryの情報
 */
export interface QueryInfo {
  columns: Array<QueryColumn>
  params: Array<QueryParam>
}

/**
 * 主キー情報
 */
export interface PrimaryKey {
  catalog: string
  schema: string
  table: string
  column: string
  seq: number
  primaryKey: string
}

/**
 * OmniDbクラス
 */
declare class OmniDb {
  private _native: any
  private _dbms: string
  private _driver: string

  constructor()

  /**
   * デバッグモードの設定
   */
  static set debug(value: boolean)

  /**
   * デバッグモードの取得
   */
  static get debug(): boolean

  /**
   * ログ取得関数を登録します。
   * @param {Function} logFunction ログ取得関数
   */
  static set logger(logger: (...args: any[]) => void)

  /**
   * ログ取得関数を取得します。
   * @returns {Function} ログ取得関数
   */
  static get logger(): (...args: any[]) => void

  /**
   * ログに日時を含めるかを設定します。
   * @param {boolean} value 日時を含めるかどうかの設定値
   */
  static set includeTimestamp(value: boolean)

  /**
   * ログに日時を含めるかの設定を取得します。
   * @returns {boolean} 日時を含めるかどうかの設定値
   */
  static get includeTimestamp(): boolean

  /**
   * ODBCドライバ一覧を取得します。
   */
  drivers(): Promise<Array<string>>

  /**
   * ODBC接続を行います。
   */
  connect(connectionString: string): Promise<boolean>

  /**
   * ODBC接続を切断します。
   */
  disconnect(): Promise<boolean>

  /**
   * 接続中のドライバ名を取得します。
   */
  driver(): string

  /**
   * 接続中のDBMS名を取得します。
   */
  dbms(): string

  /**
   * スキーマ一覧を取得します。
   */
  schemas(condition?: SchemaCondition): Promise<Array<Schema>>

  /**
   * カレントスキーマを取得します。
   */
  currentSchema(): Promise<string>

  /**
   * テーブル一覧を取得します。
   */
  tables(condition: TableCondition): Promise<Array<Table>>

  /**
   * カラム一覧を取得します。
   */
  columns(condition: ColumnCondition): Promise<Array<Column>>

  /**
   * 主キー一覧を取得します。
   */
  primaryKeys(condition?: PrimaryKeyCondition): Promise<Array<PrimaryKey>>

  /**
   * SQLの情報を返します。
   */
  query(queryString: string, options?: QueryOptions): Promise<QueryInfo>

  /**
   * SQLを実行します。
   */
  execute(sql: string): Promise<boolean>

  /**
   * SQLを実行しレコードを取得します。
   */
  records(sql: string): Promise<Array<any>>

  /**
   * ロケールを設定します。
   */
  setLocale(category: string, locale: string): boolean
}

export default OmniDb
