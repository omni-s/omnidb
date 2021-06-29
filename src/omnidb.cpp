#include <napi.h>
#include <time.h>
#include <stdlib.h>
#include <iostream>

#include "omnidb.h"
#include "nlohmann/json.hpp"

using json = nlohmann::json;

using namespace Napi;

// 
// ODBC制御
//
uv_mutex_t g_odbcMutex = {0};
SQLHENV g_hEnv = NULL;

// 文字列長
#define ODATA_LENGTH 256
#define OREMARK_LENGTH 1024

// SQLの型名
typedef struct SQLTYPENAME {
  SQLSMALLINT type;
  const SQLTCHAR *name;
} SQLTYPENAME;

// SQLのタイプ名
// https://www.ibm.com/docs/ja/i/7.3?topic=wdica-data-types-data-conversion-in-db2-i-cli-functions#rzadphddtdcn__tbcsql
// https://docs.microsoft.com/ja-jp/sql/odbc/reference/appendixes/sql-data-types?view=sql-server-ver15
const SQLTYPENAME SQLTYPENAMES[] = {
  // CHAR (n) : 固定長文字列の文字列。
  { SQL_CHAR, _O("SQL_CHAR") },
  // VARCHAR (n) : 最大文字列長 n の可変長文字列。
  { SQL_VARCHAR, _O("SQL_VARCHAR") },
  // LONG VARCHAR : 可変長文字データ。 最大長は、データソースに依存します。
  { SQL_LONGVARCHAR, _O("SQL_LONGVARCHAR") },
  // WCHAR (n) : 固定長文字列の Unicode 文字列の長さ n
  { SQL_WCHAR, _O("SQL_WCHAR") },
  // VARWCHAR (n) : 最大文字列長を持つ Unicode 可変長文字列 n
  { SQL_WVARCHAR, _O("SQL_WVARCHAR") },
  // LONGWVARCHAR : Unicode 可変長文字データ。 最大長はデータソースに依存します
  { SQL_WLONGVARCHAR, _O("SQL_WLONGVARCHAR") },
  // DECIMAL (p,s) : 少なくとも p と scale s の有効桁数を持つ符号付きの正確な数値 。
  // (最大有効桁数はドライバーで定義されています)。
  // (1 <= p <= 15;s <= p)。4/4
  { SQL_DECIMAL, _O("SQL_DECIMAL") },
  // NUMERIC (p,s) : 精度が p で小数点以下桁数が s の符号付きの正確な数値
  // (1 <= p <= 15;s <= p)。4/4
  { SQL_NUMERIC, _O("SQL_NUMERIC") },
  // SMALLINT : 精度が5および小数点以下桁数が0の numeric 値
  // (符号付き:-32768 <= n <= 32767、unsigned: 0 <= n <= 65535) [3]。
  { SQL_SMALLINT, _O("SQL_SMALLINT") },
  // INTEGER : 有効桁数が10および小数点以下桁数が0の正確な数値
  // (符号付き:-2 [31] <= n <= 2 [31]-1、符号なし: 0 <= n <= 2 [32]-1) [3]。
  { SQL_INTEGER, _O("SQL_INTEGER") },
  // real : バイナリ精度 24 (0 または絶対値が 10 [-38] ~ 10 [38]) の符号付き概数値。
  { SQL_REAL, _O("SQL_REAL") },
  // FLOAT (p) : 少なくとも p のバイナリ有効桁数を持つ、符号付きの概数型の数値。
  // (最大有効桁数はドライバーで定義されています)。5/5
  { SQL_FLOAT, _O("SQL_FLOAT") },
  // DOUBLE PRECISION : バイナリ精度 53 (0 または絶対値が 10 [-308] ~ 10 [308])
  // の符号付き概数。数値。
  { SQL_DOUBLE, _O("SQL_DOUBLE") },
  // BIT : 1ビットのバイナリデータ。8
  { SQL_BIT, _O("SQL_BIT") },
  // TINYINT : 精度3および小数点以下桁数が0の正確な数値 
  // (符号付き:-128 <= n <= 127、符号なし: 0 <= n <= 255) [3]。
  { SQL_TINYINT, _O("SQL_TINYINT") },
  // bigint : 精度が 19 (符号付きの場合) または 20 (符号なしの場合) 
  // および小数点以下桁数 0 (符号付きの場合) およびスケール 0 
  // (符号付き:-2 [63] <= n <= 2 [63]-1、符号なし: 0 <= n <= 2 [64]-1) [3]、[9]
  { SQL_BIGINT, _O("SQL_BIGINT") },
  // バイナリ (n) : 固定長 n のバイナリデータ。ませ
  { SQL_BINARY, _O("SQL_BINARY") },
  // VARBINARY (n) : 最大長 n の可変長バイナリデータ。 最大値は、ユーザーによって
  // 設定されます。
  { SQL_VARBINARY, _O("SQL_VARBINARY") },
  // LONG VARBINARY : 可変長バイナリ データ。 最大長は、データソースに依存します。
  { SQL_LONGVARBINARY, _O("SQL_LONGVARBINARY") },
  // DATE : グレゴリオ暦の規則に準拠した年、月、日の各フィールド。 
  // (この付録の後半の「 グレゴリオ暦の制約」を参照してください)。
  { SQL_TYPE_DATE, _O("SQL_TYPE_DATE") },
  // 時間 (p) : 時間、分、および秒のフィールド。有効な値は 00 ~ 23 の時間、00 ~ 59
  // の有効な値、および 00 ~ 61 の秒の有効な値です。 有効桁数 p 秒の有効桁数を示します。
  { SQL_TYPE_TIME, _O("SQL_TYPE_TIME") },
  // タイムスタンプ (p) : 日付と時刻のデータ型に対して定義されている有効な値を持つ
  // 年、月、日、時、分、および秒の各フィールド。
  { SQL_TYPE_TIMESTAMP, _O("SQL_TYPE_TIMESTAMP") },
/* unix-odbcで未定義だった
  // UTCDATETIME : Year、month、day、hour、minute、second、utchour、utcminute
  // の各フィールド。 Utchour フィールドと utcminute フィールドの精度は1/10 マイクロ秒です。
  { SQL_TYPE_UTCDATETIME, _O("SQL_TYPE_UTCDATETIME" },
  // UTCTIME : Hour、minute、second、utchour、utcminute の各フィールド。 Utchour
  // フィールドと utcminute フィールドの精度は1/10 マイクロ秒です。
  { SQL_TYPE_UTCTIME, _O("SQL_TYPE_UTCTIME" },
*/
  // 間隔月 (p) : 2つの日付の間の月数。 p は、間隔の有効桁数です。
  { SQL_INTERVAL_MONTH, _O("SQL_INTERVAL_MONTH") },
  // 間隔の年 (p) : 2つの日付間の年数 p は、間隔の有効桁数です。
  { SQL_INTERVAL_YEAR, _O("SQL_INTERVAL_YEAR") },
  // 間隔の年 (p) から月 : 2つの日付間の年と月の数。 p は、間隔の有効桁数です。
  { SQL_INTERVAL_YEAR_TO_MONTH, _O("SQL_INTERVAL_YEAR_TO_MONTH") },
  // 間隔の日 (p) : 2つの日付の間の日数 p は、間隔の有効桁数です。
  { SQL_INTERVAL_DAY, _O("SQL_INTERVAL_DAY") },
  // 間隔 (時間) (p) : 2つの日付/時刻の間の時間数。 p は、間隔の有効桁数です。
  { SQL_INTERVAL_HOUR, _O("SQL_INTERVAL_HOUR") },
  // 間隔 (分) (p) : 2つの日付/時刻の間の分数 p は、間隔の有効桁数です。
  { SQL_INTERVAL_MINUTE, _O("SQL_INTERVAL_MINUTE") },
  // INTERVAL 秒 (p,q) : 2つの日付/時刻の間の秒数。 p は間隔の先頭の有効桁数
  // で、 q は間隔の秒の有効桁数です。
  { SQL_INTERVAL_SECOND, _O("SQL_INTERVAL_SECOND") },
  // 間隔の日 (p) から時間 : 2つの日付/時刻の間の日数/時間。 p は、間隔の
  // 有効桁数です。
  { SQL_INTERVAL_DAY_TO_HOUR, _O("SQL_INTERVAL_DAY_TO_HOUR") },
  // 間隔の日 (p) から分 : 2つの日付/時刻の間の日数/時間/分 p は、間隔の
  // 有効桁数です。
  { SQL_INTERVAL_DAY_TO_MINUTE, _O("SQL_INTERVAL_DAY_TO_MINUTE") },
  // 間隔の日 (p) から秒 (q) : 2つの日付/時刻の間の日数/時間/分/秒 p は間隔
  // の先頭の有効桁数で、 q は間隔の秒の有効桁数です。
  { SQL_INTERVAL_DAY_TO_SECOND, _O("SQL_INTERVAL_DAY_TO_SECOND") },
  // INTERVAL 時間 (p) から分 : 2つの日付/時刻の間の時間数/分 p は、間隔の
  // 有効桁数です。
  { SQL_INTERVAL_HOUR_TO_MINUTE, _O("SQL_INTERVAL_HOUR_TO_MINUTE") },
  // INTERVAL 時間 (p) から秒 (q) : 2つの日付/時刻の間の時間数/分/秒。
  // p は間隔の先頭の有効桁数で、 q は間隔の秒の有効桁数です。
  { SQL_INTERVAL_HOUR_TO_SECOND, _O("SQL_INTERVAL_HOUR_TO_SECOND") },
  // 間隔 (分) (p) から秒 (q) : 2つの日付/時刻の間の分数 (秒単位)。
  // p は間隔の先頭の有効桁数で、 q は間隔の秒の有効桁数です。
  { SQL_INTERVAL_MINUTE_TO_SECOND, _O("SQL_INTERVAL_MINUTE_TO_SECOND") },
  // GUID : 固定長 GUID。
  { SQL_GUID, _O("SQL_GUID") }
};


//
// SQLHSTMTをunique_ptrの解放で使うための型
// 
struct StmtAcc {
  typedef SQLHSTMT pointer;
  // 開放時
  inline void operator()(SQLHSTMT stmt) const { if(stmt) { SQLFreeHandle(SQL_HANDLE_STMT, stmt); } }
  // アロケータ
  static SQLHSTMT alloc(SQLHANDLE odbc) {
    SQLHSTMT stmt = 0;  
    SQLAllocHandle(SQL_HANDLE_STMT, odbc, &stmt);
    return stmt;
  };
};


/**
* omnidbインスタンスの生成(newの時)
*
* @param[in] env Node.js環境
* @param[in] info コールバック
* @return Napi::Object ？
*/
Napi::Object OmniDb::NewInstance(Napi::Env env, const Napi::CallbackInfo &info)
{
  Napi::EscapableHandleScope scope(env);
  const std::initializer_list<napi_value> initArgList = {info[0]};
  Napi::Object obj = env.GetInstanceData<Napi::FunctionReference>()->New(initArgList);
  return scope.Escape(napi_value(obj)).ToObject();
}


/**
* OmniDbモジュール初期化(requireされた時)
*
* @param[in] env Node.js環境
* @param[in] exports 公開オブジェクト登録先
* @return Napi::Object 公開オブジェクト
*/
Napi::Object OmniDb::Init(Napi::Env env, Napi::Object exports)
{
  //
  // ライブラリ初期化
  //
  uv_mutex_init(&g_odbcMutex);

  uv_mutex_lock(&g_odbcMutex);
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &g_hEnv);
  uv_mutex_unlock(&g_odbcMutex);
  if (!SQL_SUCCEEDED(ret)) {
    return exports;
  }

  // ODBC 3.0
  SQLSetEnvAttr(g_hEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) SQL_OV_ODBC3, SQL_IS_UINTEGER);

  //
  // 外部公開用メソッド登録
  //
  Napi::Function func = DefineClass(
    env, "omnidb", {
      InstanceMethod("connect", &OmniDb::Connect),
      InstanceMethod("disconnect", &OmniDb::Disconnect),
      InstanceMethod("drivers", &OmniDb::Drivers),
      InstanceMethod("query", &OmniDb::Query),
      InstanceMethod("tables", &OmniDb::Tables),
      InstanceMethod("columns", &OmniDb::Columns),
  });

  Napi::FunctionReference *constructor = new Napi::FunctionReference();
  *constructor = Napi::Persistent(func);
  env.SetInstanceData(constructor);

  exports.Set("omnidb", func);
  return exports;
}

/**
* コンストラクタ
*/
OmniDb::OmniDb(const Napi::CallbackInfo &info) : Napi::ObjectWrap<OmniDb>(info)
{
  m_hOdbc = NULL;
};


/**
* デストラクタ
*/
OmniDb::~OmniDb()
{
  if (m_hOdbc) {
    _Disconnect();
  }

  uv_mutex_lock(&g_odbcMutex);

  if (g_hEnv) {
    SQLFreeHandle(SQL_HANDLE_ENV, g_hEnv);
    g_hEnv = NULL;
  }
  uv_mutex_unlock(&g_odbcMutex);  
};


/**
* 指定されたODBC接続文字列を元にDBと接続します
*
* @param[in] info Node.jsパラメータ
* @return Napi::Value 成否を返します
*/
Napi::Value OmniDb::Connect(const Napi::CallbackInfo &info)
{
  Napi::Env env = info.Env();

  //
  // connect(connectionString)
  // のパラメータチェック
  //
  if(info.Length() != 1) {
    CreateTypeError(
      env,
      OString(_O("connect(connectionString) connectionStringは必須です"))
    ).ThrowAsJavaScriptException();
    return env.Null();
  }
  if (!info[0].IsString()) {
    CreateTypeError(
      env,
      OString(_O("connect: connectionStringはstringを指定してください"))
    ).ThrowAsJavaScriptException();
    return env.Null();
  }

  Napi::String _connectionString = info[0].As<Napi::String>();
  std::unique_ptr<SQLTCHAR> connectString(OmniDb::NapiStringToSQLTCHAR(_connectionString));

  // DB接続
  // https://www.ibm.com/docs/ja/i/7.3?topic=details-connection-string-keywords
  uv_mutex_lock(&g_odbcMutex);
  SQLHDBC hOdbc;
  SQLAllocHandle(SQL_HANDLE_DBC, g_hEnv, &hOdbc);
  SQLRETURN ret = SQLDriverConnect(hOdbc, NULL, connectString.get(), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
  uv_mutex_unlock(&g_odbcMutex);
  if(!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_DBC, hOdbc);
    Napi::Error::New(env, "hoge").ThrowAsJavaScriptException();
    return env.Null();
  }
  m_hOdbc = hOdbc;

  return Napi::Boolean::New(env, true);
}


/**
* DB切断
*
* @param[in] info Node.jsパラメータ
* @return Napi::Value 成否
*/
Napi::Value OmniDb::Disconnect(const Napi::CallbackInfo &info)
{
  Napi::Env env = info.Env();

  // DB切断
  _Disconnect();

  return Napi::Boolean::New(env, true);
}

/**
* DB切断
*/
void OmniDb::_Disconnect()
{
  if(m_hOdbc) {
    SQLDisconnect(m_hOdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, m_hOdbc);
    m_hOdbc = NULL;
  }
}


/**
* ドライバ情報取得
*
* @param[in] info Node.jsパラメータ
* @return Napi::Value ドライバ情報をJSON形式の文字列で返します
*/
Napi::Value OmniDb::Drivers(const Napi::CallbackInfo &info)
{
  Napi::Env env = info.Env();
  ocout << g_hEnv << _O("drivers\n");
  
  // ドライバ情報取得
  json drivers = json::array();

	SQLTCHAR _driver[ODATA_LENGTH];
	SQLTCHAR _attribute[OREMARK_LENGTH];

	SQLSMALLINT dret, aret;
  SQLRETURN ret;

  SQLUSMALLINT direction = SQL_FETCH_FIRST;
  uv_mutex_lock(&g_odbcMutex);
  while(
    SQL_SUCCEEDED(ret = SQLDrivers(
      g_hEnv, direction,
      _driver, sizeof(_driver), &dret,
      _attribute, sizeof(_attribute), &aret))) {
// うまく動かない

    json driver = json::object();
    driver["name"] = to_jsonstr(OString(_driver));
    driver["attribute"] = to_jsonstr(OString(_attribute));
    drivers.push_back(driver);
    direction = SQL_FETCH_NEXT;
  }
  uv_mutex_unlock(&g_odbcMutex);

  //
  // JSON文字列として出力
  //
  return Napi::String::New(env, drivers.dump());
}


/**
* テーブル情報取得
*
* @param[in] info Node.jsパラメータ
* @return Napi::Value テーブル情報をJSON形式の文字列で返します
*/
Napi::Value OmniDb::Tables(const Napi::CallbackInfo& info)
{
  SQLRETURN ret;
  Napi::Env env = info.Env();
  ocout << _O("tables\n");

  std::unique_ptr<SQLTCHAR> catalog = nullptr;
  std::unique_ptr<SQLTCHAR> schema = nullptr;
  std::unique_ptr<SQLTCHAR> table = nullptr;
  std::unique_ptr<SQLTCHAR> tableType(new SQLTCHAR[256]);

  // デフォルトはテーブルのみ出力
  ostrcpy(tableType.get(), _O("TABLE"));

  //
  // tables(condition)
  // のパラメータチェック ※conditionは任意
  //
  if(info.Length() == 1) {
    // conditionがある場合は条件取得
    if(!info[0].IsObject()) {
      CreateTypeError(
        env,
        OString(_O("condition はオブジェクトのみ指定できます"))
      ).ThrowAsJavaScriptException();
      return env.Null();
    }

    // 取得条件取得
    Napi::Object condition = info[0].As<Napi::Object>();

    // カタログ（データベース条件）
    if(condition.Has("catalog")) {
      Napi::String _catalog = condition.Get("catalog").ToString();
      if(!IsBlank(_catalog))
        catalog.reset(OmniDb::NapiStringToSQLTCHAR(_catalog));
    }
    // スキーマー
    if(condition.Has("schema")) {
      Napi::String _schema = condition.Get("schema").ToString();
      if(!IsBlank(_schema))
        schema.reset(OmniDb::NapiStringToSQLTCHAR(_schema));
    }
    // テーブル
    if(condition.Has("table")) {
      Napi::String _table = condition.Get("table").ToString();
      if(!IsBlank(_table))
        table.reset(OmniDb::NapiStringToSQLTCHAR(_table));
    }
    // カラム
    if(condition.Has("tableType")) {
      Napi::String _tableType = condition.Get("tableType").ToString();
      if(!IsBlank(_tableType))
        tableType.reset(OmniDb::NapiStringToSQLTCHAR(_tableType));
    }
  }

  
  // テーブル情報取得
  // https://www.ibm.com/docs/ja/i/7.3?topic=functions-sqlcolumns-get-column-information-table
  std::unique_ptr<SQLHSTMT, StmtAcc> stmt(StmtAcc::alloc(m_hOdbc));
  if(!SQL_SUCCEEDED(ret = 
    SQLTables(
      stmt.get(),
      catalog.get(), catalog.get() == nullptr ? 0 : SQL_NTS,
      schema.get(), schema.get() == nullptr ? 0 : SQL_NTS,
      table.get(), table.get() == nullptr ? 0 : SQL_NTS,
      tableType.get(), tableType.get() == nullptr ? 0 : SQL_NTS))) {
    // ErrorMessage(_O("SQLColumns"), ret, SQL_HANDLE_STMT, stmt)
    Napi::Error::New(env, "hoge").ThrowAsJavaScriptException();
    return env.Null();
  }  

  //
  // テーブル情報を出力
  //
  std::unique_ptr<SQLTCHAR> colCatalog(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colSchema(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colTable(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colTableType(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colRemarks(new SQLTCHAR[OREMARK_LENGTH]);
  SQLLEN sizCatalog; 
  SQLLEN sizSchema;  
  SQLLEN sizTable;  
  SQLLEN sizTableType;  
  SQLLEN sizRemarks;  
  #ifdef UNICODE
  SQLSMALLINT ctype = SQL_C_WCHAR;
  #else
  SQLSMALLINT ctype = SQL_C_CHAR;
  #endif

  SQLBindCol(stmt.get(), 1, ctype, colCatalog.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizCatalog);  
  SQLBindCol(stmt.get(), 2, ctype, colSchema.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizSchema);  
  SQLBindCol(stmt.get(), 3, ctype, colTable.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizTable);  
  SQLBindCol(stmt.get(), 4, ctype, colTableType.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizTableType);  
  SQLBindCol(stmt.get(), 5, ctype, colRemarks.get(), OREMARK_LENGTH * sizeof(SQLTCHAR), &sizRemarks);  

  // 全ての列情報を出力
  json tables = json::array();
  while ((ret = SQLFetch(stmt.get())) == SQL_SUCCESS) {
    json table = json::object();
    table["catalog"] = to_jsonstr(OString(colCatalog.get()));
    table["schema"] = to_jsonstr(OString(colSchema.get()));
    table["name"] = to_jsonstr(OString(colTable.get()));
    table["type"] = to_jsonstr(OString(colTableType.get()));
    table["remarks"] = to_jsonstr(trimString(OString(colRemarks.get())));
    tables.push_back(table);
  }

  //
  // JSON文字列として返却
  //
  return Napi::String::New(env, tables.dump());
}


/**
* カラム情報取得
*
* @param[in] info Node.jsパラメータ
* @return Napi::Value カラム情報をJSON形式の文字列で返します
*/
Napi::Value OmniDb::Columns(const Napi::CallbackInfo &info)
{
  SQLRETURN ret;
  Napi::Env env = info.Env();

  std::unique_ptr<SQLTCHAR> catalog = nullptr;
  std::unique_ptr<SQLTCHAR> schema = nullptr;
  std::unique_ptr<SQLTCHAR> table = nullptr;
  std::unique_ptr<SQLTCHAR> column = nullptr;

  //
  // columns(condition)
  //
  // のパラメータチェック。conditionは任意
  //
  if(info.Length() == 1) {
    if(!info[0].IsObject()) {
      CreateTypeError(
        env,
        OString(_O("condition はオブジェクトのみ指定できます"))
      ).ThrowAsJavaScriptException();
      return env.Null();
    }

    // 取得条件取得
    Napi::Object condition = info[0].As<Napi::Object>();

    // カタログ（データベース条件）
    if(condition.Has("catalog")) {
      Napi::String _catalog = condition.Get("catalog").ToString();
      if(!IsBlank(_catalog))
        catalog.reset(OmniDb::NapiStringToSQLTCHAR(_catalog));
    }
    // スキーマー
    if(condition.Has("schema")) {
      Napi::String _schema = condition.Get("schema").ToString();
      if(!IsBlank(_schema))
        schema.reset(OmniDb::NapiStringToSQLTCHAR(_schema));
    }
    // テーブル
    if(condition.Has("table")) {
      Napi::String _table = condition.Get("table").ToString();
      if(!IsBlank(_table))
        table.reset(OmniDb::NapiStringToSQLTCHAR(_table));
    }
    // カラム
    if(condition.Has("column")) {
      Napi::String _column = condition.Get("column").ToString();
      if(!IsBlank(_column))
        column.reset(OmniDb::NapiStringToSQLTCHAR(_column));
    }
  }

  // テーブルのカラム情報取得
  // https://www.ibm.com/docs/ja/i/7.3?topic=functions-sqlcolumns-get-column-information-table
  std::unique_ptr<SQLHSTMT, StmtAcc> stmt(StmtAcc::alloc(m_hOdbc));

  if(!SQL_SUCCEEDED(ret = 
    SQLColumns(
      stmt.get(),
      catalog.get(), catalog.get() == nullptr ? 0 : SQL_NTS,
      schema.get(), schema.get() == nullptr ? 0 : SQL_NTS,
      table.get(), table.get() == nullptr ? 0 : SQL_NTS,
      column.get(), column.get() == nullptr ? 0 : SQL_NTS))) {
    // ErrorMessage(_O("SQLColumns"), ret, SQL_HANDLE_STMT, stmt)
    Napi::Error::New(env, "hoge").ThrowAsJavaScriptException();
    return env.Null();
  }  

  //
  // カラム情報を出力
  //
  std::unique_ptr<SQLTCHAR> colCatalog(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colSchema(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colTable(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colColumn(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colTypeName(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colRemarks(new SQLTCHAR[OREMARK_LENGTH]);
  std::unique_ptr<SQLTCHAR> colDefault(new SQLTCHAR[ODATA_LENGTH]);
  SQLINTEGER colSize;  
  SQLSMALLINT colDecimalDigits;  
  SQLSMALLINT colNumPrec;  
  SQLSMALLINT colNullable;  
  SQLLEN sizCatalog;  
  SQLLEN sizSchema;  
  SQLLEN sizTable;  
  SQLLEN sizColumn;  
  SQLLEN sizTypeName;  
  SQLLEN sizSize;  
  SQLLEN sizDecimalDigits;  
  SQLLEN sizNumPrec;  
  SQLLEN sizNullable;  
  SQLLEN sizRemarks;  
  SQLLEN sizDefault;  
  #ifdef UNICODE
  SQLSMALLINT ctype = SQL_C_WCHAR;
  #else
  SQLSMALLINT ctype = SQL_C_CHAR;
  #endif

  SQLBindCol(stmt.get(),  1, ctype, colCatalog.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizCatalog);  
  SQLBindCol(stmt.get(),  2, ctype, colSchema.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizSchema);  
  SQLBindCol(stmt.get(),  3, ctype, colTable.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizTable);  
  SQLBindCol(stmt.get(),  4, ctype, colColumn.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizColumn);  
  SQLBindCol(stmt.get(),  6, ctype, colTypeName.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizTypeName);  
  SQLBindCol(stmt.get(),  7, SQL_C_SLONG, &colSize, 0, &sizSize);  
  SQLBindCol(stmt.get(),  9, SQL_C_SSHORT, &colDecimalDigits, 0, &sizDecimalDigits);  
  SQLBindCol(stmt.get(), 10, SQL_C_SSHORT, &colNumPrec, 0, &sizNumPrec);  
  SQLBindCol(stmt.get(), 11, SQL_C_SSHORT, &colNullable, 0, &sizNullable);
  SQLBindCol(stmt.get(), 12, ctype, colRemarks.get(), OREMARK_LENGTH * sizeof(SQLTCHAR), &sizRemarks);  
  SQLBindCol(stmt.get(), 13, ctype, colDefault.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizDefault);  

  json cols = json::array();
  while ((ret = SQLFetch(stmt.get())) == SQL_SUCCESS) {
    json col = json::object();
    col["catalog"] = to_jsonstr(OString(colCatalog.get()));
    col["schema"] = to_jsonstr(OString(colSchema.get()));
    col["table"] = to_jsonstr(OString(colTable.get()));
    col["name"] = to_jsonstr(OString(colColumn.get()));
    col["type"] = to_jsonstr(OString(colTypeName.get()));
    col["size"] = colSize;
    col["decimalDigits"] = colDecimalDigits;
    col["numPrec"] = colNumPrec;
    col["remarks"] = to_jsonstr(trimString(OString(colRemarks.get())));
    col["defualt"] = to_jsonstr(OString(colDefault.get()));
    col["nullable"] = (colNullable == SQL_NULLABLE) ? true : false;
    cols.push_back(col);
  }

  //
  // カラム情報をJSON文字列として返却
  //
  return Napi::String::New(env, cols.dump());
}


/**
* パラメータ付きSQL文字列を解析します
*
* @param[in] info Node.jsパラメータ
* @return Napi::Value SQLの情報をJSON形式の文字列で返します
*/
Napi::Value OmniDb::Query(const Napi::CallbackInfo& info)
{
  // 出力タイプ
  #define NUM_ATTR    0   // 数値属性
  #define CHAR_ATTR   1   // キャラ属性

  typedef struct  {
    SQLSMALLINT type;
    const char *name;
    SQLSMALLINT attr;
  } QUERY_COLTYPE;

  // QUERY情報で出力するタイプ
  static const QUERY_COLTYPE QUERY_COLTYPES[] = {
    // 列名
    { SQL_DESC_NAME, "name", CHAR_ATTR },
    // ラベル名
    { SQL_DESC_LABEL, "label", CHAR_ATTR },
    // データタイプ
    { SQL_DESC_TYPE, "type", NUM_ATTR },
    // NULL
    { SQL_DESC_NULLABLE, "nullable", NUM_ATTR },
    // オートインクリメント
    { SQL_DESC_AUTO_UNIQUE_VALUE, "autoIncliment", NUM_ATTR },
    // サイズ
    { SQL_DESC_LENGTH, "size", NUM_ATTR },
    // カタログ名（物理的な割当がある場合）
    { SQL_DESC_CATALOG_NAME, "catalog", CHAR_ATTR },
    // スキーマ名（物理的な割当がある場合）
    { SQL_DESC_SCHEMA_NAME, "schema", CHAR_ATTR },
    // テーブル名（物理的な割当がある場合）
    { SQL_DESC_BASE_TABLE_NAME, "table", CHAR_ATTR },
    // カラム名（物理的な割当がある場合）
    { SQL_DESC_BASE_COLUMN_NAME, "colomn", CHAR_ATTR },
  };

  SQLRETURN ret;
  Napi::Env env = info.Env();
  
  // query(queryString)
  // のパラメータチェック
  if(info.Length() != 1) {
    CreateTypeError(
      env, 
      OString(_O("query(queryString) queryStringパラメータは必須です"))
    ).ThrowAsJavaScriptException();
    return env.Null();
  }

  if(!info[0].IsString()) {
    CreateTypeError(
      env, 
      OString(_O("queryString は文字列のみ指定できます"))
    ).ThrowAsJavaScriptException();
    return env.Null();
  }

  //
  // パラメータ付きSQLの解析
  //
  json query = json::object();

  Napi::String _queryString = info[0].As<Napi::String>();
  std::unique_ptr<SQLTCHAR> queryString(OmniDb::NapiStringToSQLTCHAR(_queryString));

  std::unique_ptr<SQLHSTMT, StmtAcc> stmt(StmtAcc::alloc(m_hOdbc));
  if(!SQL_SUCCEEDED(ret = SQLPrepare(stmt.get(), queryString.get(), SQL_NTS))) {
    return env.Null();
  }

  //
  // カラム情報の取得
  //
  json cols = json::array();
  
  SQLSMALLINT numCol;
  if(!SQL_SUCCEEDED(ret = SQLNumResultCols(stmt.get(), &numCol))) {
    // MSG(SQL_HANDLE_STMT, stmt, rc);
    // CleanODBC(env, dbc, stmt);
    return env.Null();
  }

  for(int col = 0; col < numCol; col++) {
    json column = json::object();

    SQLSMALLINT numType = sizeof(QUERY_COLTYPES) / sizeof(QUERY_COLTYPE);
    for(int t = 0; t < numType; t++) {
      // カラムの属性値取得
      SQLTCHAR data[8192];
      SQLSMALLINT dataSize;
      SQLLEN attr;
      memset(data, 0x00, sizeof(data));

      // カラム情報取得
      if(!SQL_SUCCEEDED(
        SQLColAttribute(
          stmt.get(), col + 1, QUERY_COLTYPES[t].type,
          data, sizeof(data), &dataSize, &attr))) {
        continue;
      }

      const char *prop = QUERY_COLTYPES[t].name;
      if(QUERY_COLTYPES[t].attr == NUM_ATTR) {
        // 数値データの場合は加工
        switch (QUERY_COLTYPES[t].type)
        {
          case SQL_DESC_AUTO_UNIQUE_VALUE:
            // auto-increment
            column[prop] = (attr == SQL_TRUE) ? true : false;
            break;
          case SQL_DESC_NULLABLE:
            // nullを許可するか
            column[prop] = (attr == SQL_NULLABLE) ? true : false;
            break;
          case SQL_DESC_TYPE:
            // データ型
            column[prop] = to_jsonstr(GetTypeName(attr));
            break;
          default:
            // 上記以外の数値型の場合はそのまま転送
            column[prop] = attr;
            break;
        }
      } else {
        // 文字列データの場合は加工無しで設定
        column[prop] = to_jsonstr(OString(data));
      }
    }

    cols.push_back(column);
  }

  //
  // パラメータ情報の取得
  //
  json params = json::array();

  // パラメータ数取得
  SQLSMALLINT numParam;
  if(!SQL_SUCCEEDED(ret = SQLNumParams(stmt.get(), &numParam))) {
    return env.Null();
  }

  for (int p = 0; p < numParam; p++) {
    // パラメータの情報を取得する
    // https://www.ibm.com/docs/ja/i/7.3?topic=functions-sqldescribeparam-return-description-parameter-marker
    json param = json::object();

    SQLSMALLINT dataType = 0;
    SQLULEN paramSize = 0;
    SQLSMALLINT decimalDigits = 0;
    SQLSMALLINT nullable = 0;
    if(!SQL_SUCCEEDED(ret =
      SQLDescribeParam(
        stmt.get(), p + 1, &dataType, &paramSize, &decimalDigits, &nullable))) {
      return env.Null();
    }

    // データ型
    param["type"] = to_jsonstr(GetTypeName(dataType));
    // サイズ
    param["size"] = paramSize;
    // nullを許可するか
    param["nullable"]= (nullable == SQL_NULLABLE) ? true : false;
    params.push_back(param);
  }

  //
  // SQL情報返却
  //
  json result = json::object();
  result["columns"] = cols;
  result["params"] = params;
  return Napi::String::New(env, result.dump());
}


/**
* ODBCエラー文字列取得
*/
OString OmniDb::ErrorMessage(const OString &msg, SQLRETURN retcode, SQLSMALLINT handleType, SQLHANDLE hError)
{
  OString sqlmsg;

  //
  // SQLメッセージが取得できるやつだけ取得
  //
  if(retcode == SQL_ERROR) {
    // メッセージ数取得
    SQLLEN numRecs = 0;
    SQLGetDiagField(handleType, hError, 0, SQL_DIAG_NUMBER, &numRecs, 0, 0);

    // 全メッセージ取得
    SQLTCHAR state[32], odbcmsg[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT msgLen;
    SQLINTEGER  native;
    SQLSMALLINT recNo = 1;
    while(
      recNo <= numRecs &&
      (SQLGetDiagRec(
        handleType, hError, recNo, state, &native,  
        odbcmsg, sizeof(odbcmsg), &msgLen) != SQL_NO_DATA)) {
      if (sqlmsg.length() > 0) {
        sqlmsg += _O(", ");
      }
      OString p = state;
      p += _O(":");
      p += odbcmsg;
      p += _O("(");
      p += to_ostring(native);
      p += _O(")");
      sqlmsg += p;
      recNo++;
    }
  }
  
  OString res;
  res += _O("[SQLERR]:") + to_ostring(retcode) + _O(" ") + msg;
  if(sqlmsg.length() > 0) {
    res += _O("<description> ") + sqlmsg;
  }
  return res;
}


/**
* 型エラー作成(NAPI)
*/
Napi::TypeError OmniDb::CreateTypeError(napi_env env, const OString &msg)
{
  return Napi::TypeError::New(env, to_jsonstr(msg));
}


/**
* 型エラー作成(NAPI)
*/
Napi::Error OmniDb::CreateError(napi_env env, const OString &msg)
{
  return Napi::Error::New(env, to_jsonstr(msg));
}


/**
* SQL ODBC型名を取得します
*
* @param[in] type ODBCの型
* @return OString 型名
*/
OString OmniDb::GetTypeName(SQLSMALLINT type)
{
  OString result = OString(_O(""));

  int numType = sizeof(SQLTYPENAMES) / sizeof(SQLTYPENAME);
  for(int i = 0; i < numType; i++) {
    if(SQLTYPENAMES[i].type == type) {
        result = OString(SQLTYPENAMES[i].name);
        break;
    }
  }
  return result;
}


/**
* NAPIの文字列をSQLTCHAR文字列に変換します
*
* @param[in] string NAPI文字列
* @return SQLTCHAR* 文字列
*/
SQLTCHAR* OmniDb::NapiStringToSQLTCHAR(Napi::String string)
{
  size_t byteCount = 0;

  #ifdef UNICODE
  std::u16string tempString = string.Utf16Value();
  byteCount = (tempString.length() + 1) * 2;
  #else
  std::string tempString = string.Utf8Value();
  byteCount = tempString.length() + 1;
  #endif

  SQLTCHAR *sqlString = new SQLTCHAR[byteCount];
  std::memcpy(sqlString, tempString.c_str(), byteCount);
  return sqlString;
}


/**
* NAPIの文字列がブランクかを調査します
*
* @param[in] v NAPI文字列
* @return bool ブランクかどうか
*/
bool OmniDb::IsBlank(Napi::String v)
{
  return v.IsUndefined() || v.IsNull() || v.IsEmpty();
}


/**
* OmniDbオブジェクトを生成します
*
* @param[in] info NAPIの引数
* @return Napi::Object インスタンス
*/
Napi::Object CreateObject(const Napi::CallbackInfo& info) {
  return OmniDb::NewInstance(info.Env(), info);
}


/**
* OmniDbライブラリを初期化します
*
* @param[in] env NAPIの環境
* @param[in] exports エクポート関数
* @return Napi::Object インスタンス
*/
Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  Napi::Object new_exports = Napi::Function::New(env, CreateObject);
  return OmniDb::Init(env, new_exports);
}

NODE_API_MODULE(omnidb, InitAll)
