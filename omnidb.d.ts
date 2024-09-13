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
  query(queryString: string, options?: object): Promise<any>

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

export = OmniDb
