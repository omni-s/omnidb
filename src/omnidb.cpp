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
// TODO: 今の所はなくてもいいかなとは思うが、あとでちゃんとやる
// uv_mutex_t g_odbcMutex;

// 文字列長
#define ODATA_LENGTH 256
#define OREMARK_LENGTH 1024

// SQLの型名
typedef struct SQLTYPENAME
{
  SQLSMALLINT type;
  const SQLTCHAR *name;
  const SQLTCHAR *className;
} SQLTYPENAME;

// SQLのタイプ名
// https://www.ibm.com/docs/ja/i/7.3?topic=wdica-data-types-data-conversion-in-db2-i-cli-functions#rzadphddtdcn__tbcsql
// https://docs.microsoft.com/ja-jp/sql/odbc/reference/appendixes/sql-data-types?view=sql-server-ver15
const SQLTYPENAME SQLTYPENAMES[] = {
    // CHAR (n) : 固定長文字列の文字列。
    {SQL_CHAR, (SQLTCHAR *)_O("SQL_CHAR"), (SQLTCHAR *)_O("String")},
    // VARCHAR (n) : 最大文字列長 n の可変長文字列。
    {SQL_VARCHAR, (SQLTCHAR *)_O("SQL_VARCHAR"), (SQLTCHAR *)_O("String")},
    // LONG VARCHAR : 可変長文字データ。 最大長は、データソースに依存します。
    {SQL_LONGVARCHAR, (SQLTCHAR *)_O("SQL_LONGVARCHAR"), (SQLTCHAR *)_O("String")},
    // WCHAR (n) : 固定長文字列の Unicode 文字列の長さ n
    {SQL_WCHAR, (SQLTCHAR *)_O("SQL_WCHAR"), (SQLTCHAR *)_O("String")},
    // VARWCHAR (n) : 最大文字列長を持つ Unicode 可変長文字列 n
    {SQL_WVARCHAR, (SQLTCHAR *)_O("SQL_WVARCHAR"), (SQLTCHAR *)_O("String")},
    // LONGWVARCHAR : Unicode 可変長文字データ。 最大長はデータソースに依存します
    {SQL_WLONGVARCHAR, (SQLTCHAR *)_O("SQL_WLONGVARCHAR"), (SQLTCHAR *)_O("String")},
    // DECIMAL (p,s) : 少なくとも p と scale s の有効桁数を持つ符号付きの正確な数値 。
    // (最大有効桁数はドライバーで定義されています)。
    // (1 <= p <= 15;s <= p)。4/4
    {SQL_DECIMAL, (SQLTCHAR *)_O("SQL_DECIMAL"), (SQLTCHAR *)_O("Number")},
    // NUMERIC (p,s) : 精度が p で小数点以下桁数が s の符号付きの正確な数値
    // (1 <= p <= 15;s <= p)。4/4
    {SQL_NUMERIC, (SQLTCHAR *)_O("SQL_NUMERIC"), (SQLTCHAR *)_O("Number")},
    // SMALLINT : 精度が5および小数点以下桁数が0の numeric 値
    // (符号付き:-32768 <= n <= 32767、unsigned: 0 <= n <= 65535) [3]。
    {SQL_SMALLINT, (SQLTCHAR *)_O("SQL_SMALLINT"), (SQLTCHAR *)_O("Number")},
    // INTEGER : 有効桁数が10および小数点以下桁数が0の正確な数値
    // (符号付き:-2 [31] <= n <= 2 [31]-1、符号なし: 0 <= n <= 2 [32]-1) [3]。
    {SQL_INTEGER, (SQLTCHAR *)_O("SQL_INTEGER"), (SQLTCHAR *)_O("Number")},
    // real : バイナリ精度 24 (0 または絶対値が 10 [-38] ~ 10 [38]) の符号付き概数値。
    {SQL_REAL, (SQLTCHAR *)_O("SQL_REAL"), (SQLTCHAR *)_O("Number")},
    // FLOAT (p) : 少なくとも p のバイナリ有効桁数を持つ、符号付きの概数型の数値。
    // (最大有効桁数はドライバーで定義されています)。5/5
    {SQL_FLOAT, (SQLTCHAR *)_O("SQL_FLOAT"), (SQLTCHAR *)_O("Number")},
    // DOUBLE PRECISION : バイナリ精度 53 (0 または絶対値が 10 [-308] ~ 10 [308])
    // の符号付き概数。数値。
    {SQL_DOUBLE, (SQLTCHAR *)_O("SQL_DOUBLE"), (SQLTCHAR *)_O("Number")},
    // BIT : 1ビットのバイナリデータ。8
    {SQL_BIT, (SQLTCHAR *)_O("SQL_BIT"), (SQLTCHAR *)_O("Number")},
    // TINYINT : 精度3および小数点以下桁数が0の正確な数値
    // (符号付き:-128 <= n <= 127、符号なし: 0 <= n <= 255) [3]。
    {SQL_TINYINT, (SQLTCHAR *)_O("SQL_TINYINT"), (SQLTCHAR *)_O("Number")},
    // bigint : 精度が 19 (符号付きの場合) または 20 (符号なしの場合)
    // および小数点以下桁数 0 (符号付きの場合) およびスケール 0
    // (符号付き:-2 [63] <= n <= 2 [63]-1、符号なし: 0 <= n <= 2 [64]-1) [3]、[9]
    {SQL_BIGINT, (SQLTCHAR *)_O("SQL_BIGINT"), (SQLTCHAR *)_O("Number")},
    // バイナリ (n) : 固定長 n のバイナリデータ。ませ
    {SQL_BINARY, (SQLTCHAR *)_O("SQL_BINARY"), (SQLTCHAR *)_O("Binary")},
    // VARBINARY (n) : 最大長 n の可変長バイナリデータ。 最大値は、ユーザーによって
    // 設定されます。
    {SQL_VARBINARY, (SQLTCHAR *)_O("SQL_VARBINARY"), (SQLTCHAR *)_O("Binary")},
    // LONG VARBINARY : 可変長バイナリ データ。 最大長は、データソースに依存します。
    {SQL_LONGVARBINARY, (SQLTCHAR *)_O("SQL_LONGVARBINARY"), (SQLTCHAR *)_O("Binary")},
    // DATE : グレゴリオ暦の規則に準拠した年、月、日の各フィールド。
    // (この付録の後半の「 グレゴリオ暦の制約」を参照してください)。
    {SQL_TYPE_DATE, (SQLTCHAR *)_O("SQL_TYPE_DATE"), (SQLTCHAR *)_O("Date")},
    // 時間 (p) : 時間、分、および秒のフィールド。有効な値は 00 ~ 23 の時間、00 ~ 59
    // の有効な値、および 00 ~ 61 の秒の有効な値です。 有効桁数 p 秒の有効桁数を示します。
    {SQL_TYPE_TIME, (SQLTCHAR *)_O("SQL_TYPE_TIME"), (SQLTCHAR *)_O("Time")},
    // タイムスタンプ (p) : 日付と時刻のデータ型に対して定義されている有効な値を持つ
    // 年、月、日、時、分、および秒の各フィールド。
    {SQL_TYPE_TIMESTAMP, (SQLTCHAR *)_O("SQL_TYPE_TIMESTAMP"), (SQLTCHAR *)_O("DateTime")},
    /* unix-odbcで未定義だった
      // UTCDATETIME : Year、month、day、hour、minute、second、utchour、utcminute
      // の各フィールド。 Utchour フィールドと utcminute フィールドの精度は1/10 マイクロ秒です。
      { SQL_TYPE_UTCDATETIME, (SQLTCHAR *)_O("SQL_TYPE_UTCDATETIME" },
      // UTCTIME : Hour、minute、second、utchour、utcminute の各フィールド。 Utchour
      // フィールドと utcminute フィールドの精度は1/10 マイクロ秒です。
      { SQL_TYPE_UTCTIME, (SQLTCHAR *)_O("SQL_TYPE_UTCTIME" },
    */
    // 間隔月 (p) : 2つの日付の間の月数。 p は、間隔の有効桁数です。
    {SQL_INTERVAL_MONTH, (SQLTCHAR *)_O("SQL_INTERVAL_MONTH"), (SQLTCHAR *)_O("Number")},
    // 間隔の年 (p) : 2つの日付間の年数 p は、間隔の有効桁数です。
    {SQL_INTERVAL_YEAR, (SQLTCHAR *)_O("SQL_INTERVAL_YEAR"), (SQLTCHAR *)_O("Number")},
    // 間隔の年 (p) から月 : 2つの日付間の年と月の数。 p は、間隔の有効桁数です。
    {SQL_INTERVAL_YEAR_TO_MONTH, (SQLTCHAR *)_O("SQL_INTERVAL_YEAR_TO_MONTH"), (SQLTCHAR *)_O("Number")},
    // 間隔の日 (p) : 2つの日付の間の日数 p は、間隔の有効桁数です。
    {SQL_INTERVAL_DAY, (SQLTCHAR *)_O("SQL_INTERVAL_DAY"), (SQLTCHAR *)_O("Number")},
    // 間隔 (時間) (p) : 2つの日付/時刻の間の時間数。 p は、間隔の有効桁数です。
    {SQL_INTERVAL_HOUR, (SQLTCHAR *)_O("SQL_INTERVAL_HOUR"), (SQLTCHAR *)_O("Number")},
    // 間隔 (分) (p) : 2つの日付/時刻の間の分数 p は、間隔の有効桁数です。
    {SQL_INTERVAL_MINUTE, (SQLTCHAR *)_O("SQL_INTERVAL_MINUTE"), (SQLTCHAR *)_O("Number")},
    // INTERVAL 秒 (p,q) : 2つの日付/時刻の間の秒数。 p は間隔の先頭の有効桁数
    // で、 q は間隔の秒の有効桁数です。
    {SQL_INTERVAL_SECOND, (SQLTCHAR *)_O("SQL_INTERVAL_SECOND"), (SQLTCHAR *)_O("Number")},
    // 間隔の日 (p) から時間 : 2つの日付/時刻の間の日数/時間。 p は、間隔の
    // 有効桁数です。
    {SQL_INTERVAL_DAY_TO_HOUR, (SQLTCHAR *)_O("SQL_INTERVAL_DAY_TO_HOUR"), (SQLTCHAR *)_O("Number")},
    // 間隔の日 (p) から分 : 2つの日付/時刻の間の日数/時間/分 p は、間隔の
    // 有効桁数です。
    {SQL_INTERVAL_DAY_TO_MINUTE, (SQLTCHAR *)_O("SQL_INTERVAL_DAY_TO_MINUTE"), (SQLTCHAR *)_O("Number")},
    // 間隔の日 (p) から秒 (q) : 2つの日付/時刻の間の日数/時間/分/秒 p は間隔
    // の先頭の有効桁数で、 q は間隔の秒の有効桁数です。
    {SQL_INTERVAL_DAY_TO_SECOND, (SQLTCHAR *)_O("SQL_INTERVAL_DAY_TO_SECOND"), (SQLTCHAR *)_O("Number")},
    // INTERVAL 時間 (p) から分 : 2つの日付/時刻の間の時間数/分 p は、間隔の
    // 有効桁数です。
    {SQL_INTERVAL_HOUR_TO_MINUTE, (SQLTCHAR *)_O("SQL_INTERVAL_HOUR_TO_MINUTE"), (SQLTCHAR *)_O("Number")},
    // INTERVAL 時間 (p) から秒 (q) : 2つの日付/時刻の間の時間数/分/秒。
    // p は間隔の先頭の有効桁数で、 q は間隔の秒の有効桁数です。
    {SQL_INTERVAL_HOUR_TO_SECOND, (SQLTCHAR *)_O("SQL_INTERVAL_HOUR_TO_SECOND"), (SQLTCHAR *)_O("Number")},
    // 間隔 (分) (p) から秒 (q) : 2つの日付/時刻の間の分数 (秒単位)。
    // p は間隔の先頭の有効桁数で、 q は間隔の秒の有効桁数です。
    {SQL_INTERVAL_MINUTE_TO_SECOND, (SQLTCHAR *)_O("SQL_INTERVAL_MINUTE_TO_SECOND"), (SQLTCHAR *)_O("Number")},
    // GUID : 固定長 GUID。
    {SQL_GUID, (SQLTCHAR *)_O("SQL_GUID"), (SQLTCHAR *)_O("Guid")}};

// ロケールのカテゴリ名
typedef struct LOCALE_NAME
{
  int category;
  const SQLTCHAR *name;
} LOCALE_NAME;

const LOCALE_NAME LOCALE_NAMES[] = {
    {LC_ALL, (const SQLTCHAR *)_O("LC_ALL")},
    {LC_COLLATE, (const SQLTCHAR *)_O("LC_COLLATE")},
    {LC_CTYPE, (const SQLTCHAR *)_O("LC_CTYPE")},
    {LC_MONETARY, (const SQLTCHAR *)_O("LC_ALL")},
    {LC_NUMERIC, (const SQLTCHAR *)_O("LC_NUMERIC")},
    {LC_TIME, (const SQLTCHAR *)_O("LC_TIME")},
};

//
// SQLHSTMTをunique_ptrの解放で使うための型
//
struct StmtAcc
{
  typedef SQLHSTMT pointer;
  // 開放時
  inline void operator()(SQLHSTMT stmt) const
  {
    if (stmt)
    {
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
  }
  // アロケータ
  static SQLHSTMT alloc(SQLHANDLE odbc)
  {
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
  // TODO:初期化はここで。ただlockとunlockは必要な箇所で、、、後でちゃんとやる
  // uv_mutex_init(&g_odbcMutex);
  // uv_mutex_lock(&g_odbcMutex);
  // uv_mutex_unlock(&g_odbcMutex);

  //
  // 外部公開用メソッド登録
  //
  Napi::Function func = DefineClass(
      env, "omnidb", {
                         InstanceMethod("connect", &OmniDb::Connect),
                         InstanceMethod("disconnect", &OmniDb::Disconnect),
                         InstanceMethod("drivers", &OmniDb::Drivers),
                         InstanceMethod("driver", &OmniDb::Driver),
                         InstanceMethod("dbms", &OmniDb::Dbms),
                         InstanceMethod("query", &OmniDb::Query),
                         InstanceMethod("execute", &OmniDb::Execute),
                         InstanceMethod("records", &OmniDb::Records),
                         InstanceMethod("tables", &OmniDb::Tables),
                         InstanceMethod("columns", &OmniDb::Columns),
                         InstanceMethod("primaryKeys", &OmniDb::PrimaryKeys),
                         InstanceMethod("setLocale", &OmniDb::SetLocale),
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
  m_hEnv = NULL;
  m_hOdbc = NULL;

  //
  // ライブラリ初期化
  //
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_hEnv);
  // ODBC 3.0
  SQLSetEnvAttr(m_hEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_UINTEGER);
};

/**
 * デストラクタ
 */
OmniDb::~OmniDb()
{
  if (m_hOdbc)
  {
    _Disconnect();
  }

  if (m_hEnv)
  {
    SQLFreeHandle(SQL_HANDLE_ENV, m_hEnv);
    m_hEnv = NULL;
  }
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
  if (info.Length() != 1)
  {
    CreateTypeError(
        env,
        OString(_O("connect(connectionString) connectionStringは必須です")))
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  if (!info[0].IsString())
  {
    CreateTypeError(
        env,
        OString(_O("connect: connectionStringはstringを指定してください")))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  // 接続している状態で呼ばれた場合は一旦切断
  _Disconnect();

  Napi::String _connectionString = info[0].As<Napi::String>();
  std::unique_ptr<SQLTCHAR> connectString(OmniDb::NapiStringToSQLTCHAR(_connectionString));

  // DB接続
  // https://www.ibm.com/docs/ja/i/7.3?topic=details-connection-string-keywords
  SQLHDBC hOdbc;
  SQLAllocHandle(SQL_HANDLE_DBC, m_hEnv, &hOdbc);
  SQLRETURN ret = SQLDriverConnect(hOdbc, NULL, connectString.get(), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
  if (!SQL_SUCCEEDED(ret))
  {
    OString e = ErrorMessage(_O("SQLDriverConnect"), ret, SQL_HANDLE_DBC, hOdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hOdbc);
    CreateError(env, e).ThrowAsJavaScriptException();
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
  if (m_hOdbc)
  {
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

  // ドライバ情報取得
  json drivers = json::array();

  SQLTCHAR _driver[ODATA_LENGTH];
  SQLTCHAR _attribute[OREMARK_LENGTH];

  SQLSMALLINT dret, aret;
  SQLRETURN ret;

  SQLUSMALLINT direction = SQL_FETCH_FIRST;
  while (
      SQL_SUCCEEDED(ret = SQLDrivers(
                        m_hEnv, direction,
                        _driver, sizeof(_driver), &dret,
                        _attribute, sizeof(_attribute), &aret)))
  {
    // うまく動かない?
    json driver = json::object();
    driver["name"] = to_jsonstr(_S2O(_driver));
    driver["attribute"] = to_jsonstr(_S2O(_attribute));
    drivers.push_back(driver);
    direction = SQL_FETCH_NEXT;
  }

  //
  // JSON文字列として出力
  //
  return Napi::String::New(env, drivers.dump(-1, ' ', true, json::error_handler_t::replace));
}

/**
 * 接続しているドライバ名を取得します
 *
 * @param[in] info Node.jsパラメータ
 * @return Napi::Value ドライバ名をJSON形式の文字列で返します
 */
Napi::Value OmniDb::Driver(const Napi::CallbackInfo &info)
{
  SQLRETURN ret;
  Napi::Env env = info.Env();

  SQLTCHAR driverName[1024];
  SQLSMALLINT driverNameLength = 0;
  memset(driverName, 0x00, sizeof(driverName));

  if (m_hOdbc)
  {
    // 接続しているドライバのファイル名を取得
    if (!SQL_SUCCEEDED(ret =
                           SQLGetInfo(m_hOdbc, SQL_DRIVER_NAME, driverName, sizeof(driverName), &driverNameLength)))
    {
      CreateError(
          env,
          ErrorMessage(_O("SQLGetInfo"), ret, SQL_HANDLE_DBC, m_hOdbc))
          .ThrowAsJavaScriptException();
      return env.Null();
    }
  }

  return Napi::String::New(env, _N(driverName));
}

/**
 * 接続しているDBMS名を取得します
 *
 * @param[in] info Node.jsパラメータ
 * @return Napi::Value 接続しているDBMS名を返します
 */
Napi::Value OmniDb::Dbms(const Napi::CallbackInfo &info)
{
  SQLRETURN ret;
  Napi::Env env = info.Env();

  SQLTCHAR dbmsName[1024];
  SQLSMALLINT dbmsNameLength = 0;
  memset(dbmsName, 0x00, sizeof(dbmsName));

  if (m_hOdbc)
  {
    // 接続しているDBMS名を取得
    // https://learn.microsoft.com/ja-jp/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver16
    //
    // 2023/11/25現在
    // PostgreSQL=PostgreSQL～
    // MySQL=MySQL
    // MariaDB=MariaDB
    // SQL Server=Microsoft SQL Server
    // IBMI=DB2/400～
    if (!SQL_SUCCEEDED(ret =
                           SQLGetInfo(m_hOdbc, SQL_DBMS_NAME, dbmsName, sizeof(dbmsName), &dbmsNameLength)))
    {
      CreateError(
          env,
          ErrorMessage(_O("SQLGetInfo"), ret, SQL_HANDLE_DBC, m_hOdbc))
          .ThrowAsJavaScriptException();
      return env.Null();
    }
  }

  return Napi::String::New(env, _N(dbmsName));
}

/**
 * テーブル情報取得
 *
 * @param[in] info Node.jsパラメータ
 * @return Napi::Value テーブル情報をJSON形式の文字列で返します
 */
Napi::Value OmniDb::Tables(const Napi::CallbackInfo &info)
{
  SQLRETURN ret;
  Napi::Env env = info.Env();

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
  if (info.Length() == 1)
  {
    // conditionがある場合は条件取得
    if (!info[0].IsObject())
    {
      CreateTypeError(
          env,
          OString(_O("condition はオブジェクトのみ指定できます")))
          .ThrowAsJavaScriptException();
      return env.Null();
    }

    // 取得条件取得
    Napi::Object condition = info[0].As<Napi::Object>();

    // カタログ（データベース条件）
    if (condition.Has("catalog"))
    {
      Napi::String _catalog = condition.Get("catalog").ToString();
      if (!IsBlank(_catalog))
      {
        catalog.reset(OmniDb::NapiStringToSQLTCHAR(_catalog));
      }
    }
    // スキーマー
    if (condition.Has("schema"))
    {
      Napi::String _schema = condition.Get("schema").ToString();
      if (!IsBlank(_schema))
      {
        schema.reset(OmniDb::NapiStringToSQLTCHAR(_schema));
      }
    }
    // テーブル
    if (condition.Has("table"))
    {
      Napi::String _table = condition.Get("table").ToString();
      if (!IsBlank(_table))
      {
        table.reset(OmniDb::NapiStringToSQLTCHAR(_table));
      }
    }
    // カラム
    if (condition.Has("tableType"))
    {
      Napi::String _tableType = condition.Get("tableType").ToString();
      if (!IsBlank(_tableType))
      {
        tableType.reset(OmniDb::NapiStringToSQLTCHAR(_tableType));
      }
      else
      {
        // 空文字列の場合はNULLにする
        tableType.get()[0] = '\0';
      }
    }
  }

  // テーブル情報取得
  // https://www.ibm.com/docs/ja/i/7.3?topic=functions-sqlcolumns-get-column-information-table
  std::unique_ptr<SQLHSTMT, StmtAcc> stmt(StmtAcc::alloc(m_hOdbc));
  if (!SQL_SUCCEEDED(ret =
                         SQLTables(
                             stmt.get(),
                             catalog.get(), catalog.get() == nullptr ? 0 : SQL_NTS,
                             schema.get(), schema.get() == nullptr ? 0 : SQL_NTS,
                             table.get(), table.get() == nullptr ? 0 : SQL_NTS,
                             tableType.get(), tableType.get() == nullptr ? 0 : SQL_NTS)))
  {
    CreateError(
        env,
        ErrorMessage(_O("SQLTables"), ret, SQL_HANDLE_STMT, stmt.get()))
        .ThrowAsJavaScriptException();
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
  SQLTCHAR *emp = (SQLTCHAR *)_O("");
  while ((ret = SQLFetch(stmt.get())) == SQL_SUCCESS)
  {
    json table = json::object();
    table["catalog"] = to_jsonstr(_S2O(sizCatalog > 0 ? colCatalog.get() : emp));
    table["schema"] = to_jsonstr(_S2O(sizSchema > 0 ? colSchema.get() : emp));
    table["name"] = to_jsonstr(_S2O(sizTable > 0 ? colTable.get() : emp));
    table["type"] = to_jsonstr(_S2O(sizTableType > 0 ? colTableType.get() : emp));
    table["remarks"] = to_jsonstr(trimString(_S2O(sizRemarks > 0 ? colRemarks.get() : emp)));
    tables.push_back(table);
  }

  //
  // JSON文字列として返却
  //
  return Napi::String::New(env, tables.dump(-1, ' ', true, json::error_handler_t::replace));
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
  if (info.Length() == 1)
  {
    if (!info[0].IsObject())
    {
      CreateTypeError(
          env,
          OString(_O("condition はオブジェクトのみ指定できます")))
          .ThrowAsJavaScriptException();
      return env.Null();
    }

    // 取得条件取得
    Napi::Object condition = info[0].As<Napi::Object>();

    // カタログ（データベース条件）
    if (condition.Has("catalog"))
    {
      Napi::String _catalog = condition.Get("catalog").ToString();
      if (!IsBlank(_catalog))
        catalog.reset(OmniDb::NapiStringToSQLTCHAR(_catalog));
    }
    // スキーマー
    if (condition.Has("schema"))
    {
      Napi::String _schema = condition.Get("schema").ToString();
      if (!IsBlank(_schema))
        schema.reset(OmniDb::NapiStringToSQLTCHAR(_schema));
    }
    // テーブル
    if (condition.Has("table"))
    {
      Napi::String _table = condition.Get("table").ToString();
      if (!IsBlank(_table))
        table.reset(OmniDb::NapiStringToSQLTCHAR(_table));
    }
    // カラム
    if (condition.Has("column"))
    {
      Napi::String _column = condition.Get("column").ToString();
      if (!IsBlank(_column))
        column.reset(OmniDb::NapiStringToSQLTCHAR(_column));
    }
  }

  // テーブルのカラム情報取得
  // https://www.ibm.com/docs/ja/i/7.3?topic=functions-sqlcolumns-get-column-information-table
  std::unique_ptr<SQLHSTMT, StmtAcc> stmt(StmtAcc::alloc(m_hOdbc));

  if (!SQL_SUCCEEDED(ret =
                         SQLColumns(
                             stmt.get(),
                             catalog.get(), catalog.get() == nullptr ? 0 : SQL_NTS,
                             schema.get(), schema.get() == nullptr ? 0 : SQL_NTS,
                             table.get(), table.get() == nullptr ? 0 : SQL_NTS,
                             column.get(), column.get() == nullptr ? 0 : SQL_NTS)))
  {
    CreateError(
        env,
        ErrorMessage(_O("SQLColumns"), ret, SQL_HANDLE_STMT, stmt.get()))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  //
  // カラム情報を出力
  //
  std::unique_ptr<SQLTCHAR> colCatalog(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colSchema(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colTable(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colColumn(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colRemarks(new SQLTCHAR[OREMARK_LENGTH]);
  std::unique_ptr<SQLTCHAR> colDefault(new SQLTCHAR[ODATA_LENGTH]);
  SQLINTEGER colSize = 0;
  SQLSMALLINT colType = 0;
  SQLSMALLINT colDecimalDigits = 0;
  SQLSMALLINT colNumPrec = 0;
  SQLSMALLINT colNullable = 0;
  SQLLEN sizCatalog;
  SQLLEN sizSchema;
  SQLLEN sizTable;
  SQLLEN sizColumn;
  SQLLEN sizType;
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

  SQLBindCol(stmt.get(), 1, ctype, colCatalog.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizCatalog);
  SQLBindCol(stmt.get(), 2, ctype, colSchema.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizSchema);
  SQLBindCol(stmt.get(), 3, ctype, colTable.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizTable);
  SQLBindCol(stmt.get(), 4, ctype, colColumn.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizColumn);
  SQLBindCol(stmt.get(), 5, SQL_C_SLONG, &colType, 0, &sizType);
  SQLBindCol(stmt.get(), 7, SQL_C_SLONG, &colSize, 0, &sizSize);
  SQLBindCol(stmt.get(), 9, SQL_C_SSHORT, &colDecimalDigits, 0, &sizDecimalDigits);
  SQLBindCol(stmt.get(), 10, SQL_C_SSHORT, &colNumPrec, 0, &sizNumPrec);
  SQLBindCol(stmt.get(), 11, SQL_C_SSHORT, &colNullable, 0, &sizNullable);
  SQLBindCol(stmt.get(), 12, ctype, colRemarks.get(), OREMARK_LENGTH * sizeof(SQLTCHAR), &sizRemarks);
  SQLBindCol(stmt.get(), 13, ctype, colDefault.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizDefault);

  json cols = json::array();
  SQLTCHAR *emp = (SQLTCHAR *)_O("");

  while ((ret = SQLFetch(stmt.get())) == SQL_SUCCESS)
  {
    json col = json::object();
    col["catalog"] = to_jsonstr(_S2O(sizCatalog > 0 ? colCatalog.get() : emp));
    col["schema"] = to_jsonstr(_S2O(sizSchema > 0 ? colSchema.get() : emp));
    col["table"] = to_jsonstr(_S2O(sizTable > 0 ? colTable.get() : emp));
    col["name"] = to_jsonstr(_S2O(sizColumn > 0 ? colColumn.get() : emp));
    col["type"] = to_jsonstr(GetTypeName(colType));
    col["typeClass"] = to_jsonstr(GetTypeClassName(colType));
    col["size"] = colSize;
    col["decimalDigits"] = colDecimalDigits;
    col["numPrec"] = colNumPrec;
    col["remarks"] = to_jsonstr(trimString(_S2O(sizRemarks > 0 ? colRemarks.get() : emp)));
    col["defualt"] = to_jsonstr(_S2O(sizDefault > 0 ? colDefault.get() : emp));
    col["nullable"] = (colNullable == SQL_NULLABLE) ? true : false;
    cols.push_back(col);

    // 念のため初期化
    colType = 0;
    colSize = 0;
    colDecimalDigits = 0;
    colNumPrec = 0;
    colNullable = 0;
  }

  //
  // カラム情報をJSON文字列として返却
  //
  return Napi::String::New(env, cols.dump(-1, ' ', true, json::error_handler_t::replace));
}

/**
 * 主キー情報取得
 *
 * @param[in] info Node.jsパラメータ
 * @return Napi::Value カラム情報をJSON形式の文字列で返します
 */
Napi::Value OmniDb::PrimaryKeys(const Napi::CallbackInfo &info)
{
  SQLRETURN ret;
  Napi::Env env = info.Env();

  std::unique_ptr<SQLTCHAR> catalog = nullptr;
  std::unique_ptr<SQLTCHAR> schema = nullptr;
  std::unique_ptr<SQLTCHAR> table = nullptr;

  //
  // table(condition)
  //
  // のパラメータチェック。conditionは任意
  //
  if (info.Length() == 1)
  {
    if (!info[0].IsObject())
    {
      CreateTypeError(
          env,
          OString(_O("condition はオブジェクトのみ指定できます")))
          .ThrowAsJavaScriptException();
      return env.Null();
    }

    // 取得条件取得
    Napi::Object condition = info[0].As<Napi::Object>();

    // カタログ（データベース条件）
    if (condition.Has("catalog"))
    {
      Napi::String _catalog = condition.Get("catalog").ToString();
      if (!IsBlank(_catalog))
        catalog.reset(OmniDb::NapiStringToSQLTCHAR(_catalog));
    }
    // スキーマー
    if (condition.Has("schema"))
    {
      Napi::String _schema = condition.Get("schema").ToString();
      if (!IsBlank(_schema))
        schema.reset(OmniDb::NapiStringToSQLTCHAR(_schema));
    }
    // テーブル
    if (condition.Has("table"))
    {
      Napi::String _table = condition.Get("table").ToString();
      if (!IsBlank(_table))
        table.reset(OmniDb::NapiStringToSQLTCHAR(_table));
    }
  }

  // テーブルの主キー情報取得
  std::unique_ptr<SQLHSTMT, StmtAcc> stmt(StmtAcc::alloc(m_hOdbc));

  if (!SQL_SUCCEEDED(ret =
                         SQLPrimaryKeys(
                             stmt.get(),
                             catalog.get(), catalog.get() == nullptr ? 0 : SQL_NTS,
                             schema.get(), schema.get() == nullptr ? 0 : SQL_NTS,
                             table.get(), table.get() == nullptr ? 0 : SQL_NTS)))
  {
    CreateError(
        env,
        ErrorMessage(_O("SQLPrimaryKeys"), ret, SQL_HANDLE_STMT, stmt.get()))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  //
  // カラム情報を出力
  //
  std::unique_ptr<SQLTCHAR> colCatalog(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colSchema(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colTable(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colColumn(new SQLTCHAR[ODATA_LENGTH]);
  std::unique_ptr<SQLTCHAR> colPrimaryKey(new SQLTCHAR[ODATA_LENGTH]);
  SQLSMALLINT colKeySEQ = 0;
  SQLLEN sizCatalog;
  SQLLEN sizSchema;
  SQLLEN sizTable;
  SQLLEN sizColumn;
  SQLLEN sizKeySEQ;
  SQLLEN sizPrimaryKey;
#ifdef UNICODE
  SQLSMALLINT ctype = SQL_C_WCHAR;
#else
  SQLSMALLINT ctype = SQL_C_CHAR;
#endif

  SQLBindCol(stmt.get(), 1, ctype, colCatalog.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizCatalog);
  SQLBindCol(stmt.get(), 2, ctype, colSchema.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizSchema);
  SQLBindCol(stmt.get(), 3, ctype, colTable.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizTable);
  SQLBindCol(stmt.get(), 4, ctype, colColumn.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizColumn);
  SQLBindCol(stmt.get(), 5, SQL_C_SSHORT, &colKeySEQ, 0, &sizKeySEQ);
  SQLBindCol(stmt.get(), 6, ctype, colPrimaryKey.get(), ODATA_LENGTH * sizeof(SQLTCHAR), &sizPrimaryKey);

  json primaryKeys = json::array();
  SQLTCHAR *emp = (SQLTCHAR *)_O("");

  while ((ret = SQLFetch(stmt.get())) == SQL_SUCCESS)
  {
    json pk = json::object();
    pk["catalog"] = to_jsonstr(_S2O(sizCatalog > 0 ? colCatalog.get() : emp));
    pk["schema"] = to_jsonstr(_S2O(sizSchema > 0 ? colSchema.get() : emp));
    pk["table"] = to_jsonstr(_S2O(sizTable > 0 ? colTable.get() : emp));
    pk["column"] = to_jsonstr(_S2O(sizColumn > 0 ? colColumn.get() : emp));
    pk["seq"] = colKeySEQ;
    pk["primaryKey"] = to_jsonstr(_S2O(sizPrimaryKey > 0 ? colPrimaryKey.get() : emp));
    primaryKeys.push_back(pk);

    // 念のため初期化
    colKeySEQ = 0;
  }

  //
  // キー情報をJSON文字列として返却
  //
  return Napi::String::New(env, primaryKeys.dump(-1, ' ', true, json::error_handler_t::replace));
}

/**
 * パラメータ付きSQL文字列を解析します
 *
 * @param[in] info Node.jsパラメータ
 * @return Napi::Value SQLの情報をJSON形式の文字列で返します
 */
Napi::Value OmniDb::Query(const Napi::CallbackInfo &info)
{
// 出力タイプ
#define NUM_ATTR 0  // 数値属性
#define CHAR_ATTR 1 // キャラ属性

  typedef struct
  {
    SQLSMALLINT type;
    const char *name;
    SQLSMALLINT attr;
  } QUERY_COLTYPE;

  // QUERY情報で出力するタイプ
  static const QUERY_COLTYPE QUERY_COLTYPES[] = {
      // 列名
      {SQL_DESC_NAME, "name", CHAR_ATTR},
      // ラベル名
      {SQL_DESC_LABEL, "label", CHAR_ATTR},
      // データタイプ
      {SQL_DESC_TYPE, "type", NUM_ATTR},
      // NULL
      {SQL_DESC_NULLABLE, "nullable", NUM_ATTR},
      // オートインクリメント
      {SQL_DESC_AUTO_UNIQUE_VALUE, "autoIncliment", NUM_ATTR},
      // サイズ
      {SQL_DESC_LENGTH, "size", NUM_ATTR},
      // 10進数精度
      {SQL_DESC_SCALE, "decimalDigits", NUM_ATTR},
      // カタログ名（物理的な割当がある場合）
      {SQL_DESC_CATALOG_NAME, "catalog", CHAR_ATTR},
      // スキーマ名（物理的な割当がある場合）
      {SQL_DESC_SCHEMA_NAME, "schema", CHAR_ATTR},
      // テーブル名（物理的な割当がある場合）
      {SQL_DESC_BASE_TABLE_NAME, "table", CHAR_ATTR},
      // カラム名（物理的な割当がある場合）
      {SQL_DESC_BASE_COLUMN_NAME, "column", CHAR_ATTR},
  };

  SQLRETURN ret;
  Napi::Env env = info.Env();

  // query(queryString, options)
  // のパラメータチェック ※optionsは任意
  if (info.Length() < 1)
  {
    CreateTypeError(
        env,
        OString(_O("query(queryString) queryStringパラメータは必須です")))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[0].IsString())
  {
    CreateTypeError(
        env,
        OString(_O("queryString は文字列のみ指定できます")))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  // options
  bool option = (info.Length() >= 2 && !info[1].IsUndefined());
  if (option && !info[1].IsObject() && !info[1].IsNull())
  {
    CreateTypeError(
        env,
        OString(_O("options はオブジェクトのみ指定できます")))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  //
  // オプション取得
  //

  // ラベルオプション
  // ※LabelはibmiのANSIドライバだとうまく動かない
  bool supportLabel = false;
  if (option)
  {
    // 取得条件取得
    Napi::Object options = info[1].As<Napi::Object>();

    // ラベルオプション
    if (options.Has("label"))
    {
      Napi::Boolean _label = options.Get("label").ToBoolean();
      if (_label == true)
      {
        supportLabel = true;
      }
    }
  }

  //
  // パラメータ付きSQLの解析
  //
  json query = json::object();

  Napi::String _queryString = info[0].As<Napi::String>();
  std::unique_ptr<SQLTCHAR> queryString(OmniDb::NapiStringToSQLTCHAR(_queryString));

  std::unique_ptr<SQLHSTMT, StmtAcc> stmt(StmtAcc::alloc(m_hOdbc));
  if (!SQL_SUCCEEDED(ret = SQLPrepare(stmt.get(), queryString.get(), SQL_NTS)))
  {
    CreateError(
        env,
        ErrorMessage(_O("SQLPrepare"), ret, SQL_HANDLE_STMT, stmt.get()))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  //
  // カラム情報の取得
  //
  json cols = json::array();

  SQLSMALLINT numCol;
  if (!SQL_SUCCEEDED(ret = SQLNumResultCols(stmt.get(), &numCol)))
  {
    CreateError(
        env,
        ErrorMessage(_O("SQLNumResultCols"), ret, SQL_HANDLE_STMT, stmt.get()))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  for (int col = 0; col < numCol; col++)
  {
    json column = json::object();

    SQLTCHAR colName[255] = {0};
    SQLSMALLINT colType;
    SQLULEN colLength;
    // カラム情報取得
    // ※time,timestamp型がSQLColAttributeだと取得できないのでSQLDescribeColを使用
    if (!SQL_SUCCEEDED(ret =
                           SQLDescribeCol(stmt.get(), col + 1, colName, sizeof(colName), NULL, &colType, &colLength, NULL, NULL)))
    {
      CreateError(
          env,
          ErrorMessage(_O("SQLDescribeCol"), ret, SQL_HANDLE_STMT, stmt.get()))
          .ThrowAsJavaScriptException();
      return env.Null();
    }

    SQLSMALLINT numType = sizeof(QUERY_COLTYPES) / sizeof(QUERY_COLTYPE);
    for (int t = 0; t < numType; t++)
    {
      if ((QUERY_COLTYPES[t].type == SQL_DESC_LABEL) && !supportLabel)
      {
        // ラベルサポートなしの場合は出力しない
        continue;
      }

      // カラムの属性値取得
      SQLTCHAR data[8192];
      SQLSMALLINT dataSize = 0;
      SQLLEN attr = 0;
      memset(data, 0x00, sizeof(data));

      if (QUERY_COLTYPES[t].type == SQL_DESC_TYPE)
      {
        // データ型の場合はSQLDescribeColで取得した値を使用
        attr = colType;
      }
      else
      {
        // カラム情報取得
        if (!SQL_SUCCEEDED(
                SQLColAttribute(
                    stmt.get(), col + 1, QUERY_COLTYPES[t].type,
                    data, sizeof(data), &dataSize, &attr)))
        {
          continue;
        }
      }

      const char *prop = QUERY_COLTYPES[t].name;
      if (QUERY_COLTYPES[t].attr == NUM_ATTR)
      {
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
          // 型は特別に型クラスも出力
          column["typeClass"] = to_jsonstr(GetTypeClassName(attr));
          break;
        default:
          // 上記以外の数値型の場合はそのまま転送
          column[prop] = attr;
          break;
        }
      }
      else
      {
        // 文字列データの場合は加工無しで設定
        column[prop] = to_jsonstr(_S2O(data));
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
  if (!SQL_SUCCEEDED(ret = SQLNumParams(stmt.get(), &numParam)))
  {
    CreateError(
        env,
        ErrorMessage(_O("SQLNumParams"), ret, SQL_HANDLE_STMT, stmt.get()))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  SQLUSMALLINT supported = SQL_TRUE;
  if (!SQL_SUCCEEDED(ret = SQLGetFunctions(m_hOdbc, SQL_API_SQLDESCRIBEPARAM, &supported)))
  {
    CreateError(
        env,
        ErrorMessage(_O("SQLGetFunctions"), ret, SQL_HANDLE_DBC, m_hOdbc))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  for (int p = 0; p < numParam; p++)
  {
    // パラメータの情報を取得する
    // https://www.ibm.com/docs/ja/i/7.3?topic=functions-sqldescribeparam-return-description-parameter-marker
    json param = json::object();

    SQLSMALLINT dataType = 0;
    SQLULEN paramSize = 0;
    SQLSMALLINT decimalDigits = 0;
    SQLSMALLINT nullable = 0;

    if (supported == SQL_TRUE)
    {
      // SQLDescribeParamがサポートされている場合はSQLDescribeParamで取得
      if (!SQL_SUCCEEDED(ret =
                             SQLDescribeParam(
                                 stmt.get(), p + 1, &dataType, &paramSize, &decimalDigits, &nullable)))
      {
        CreateError(
            env,
            ErrorMessage(_O("SQLDescribeParam"), ret, SQL_HANDLE_STMT, stmt.get()))
            .ThrowAsJavaScriptException();
        return env.Null();
      }
    }
    else
    {
      // SQLDescribeParamがサポートされていない場合は強制VARCHAR(8000)とする
      dataType = SQL_VARCHAR;
      paramSize = 8000;
    }

    // データ型
    param["type"] = to_jsonstr(GetTypeName(dataType));
    // 型分類
    param["typeClass"] = to_jsonstr(GetTypeClassName(dataType));
    // サイズ
    param["size"] = paramSize;
    // 10進数
    param["decimalDigits"] = decimalDigits;
    // nullを許可するか
    param["nullable"] = (nullable == SQL_NULLABLE) ? true : false;

    params.push_back(param);
  }

  //
  // SQL情報返却
  //
  json result = json::object();
  result["columns"] = cols;
  result["params"] = params;
  return Napi::String::New(env, result.dump(-1, ' ', true, json::error_handler_t::replace));
}

/**
 * SQLを直接実行します。結果は成否のみ返します
 *
 * @param[in] info Node.jsパラメータ
 * @return Napi::Value SQLの情報をJSON形式の文字列で返します
 */
Napi::Value OmniDb::Execute(const Napi::CallbackInfo &info)
{
  SQLRETURN ret;
  Napi::Env env = info.Env();

  //
  // execute(sql)
  //
  // のパラメータチェック
  //
  if (info.Length() < 1)
  {
    CreateTypeError(
        env,
        OString(_O("execute(sql) sqlパラメータは必須です")))
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  if (!info[0].IsString())
  {
    CreateTypeError(
        env,
        OString(_O("sql は文字列のみ指定できます")))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  //
  // 指定されたSQLを実行するだけ。例外がでなければ成功
  //
  // omnidbのSQL実行はテンポラリテーブルやライブラリリスト等の前準備として必要なもの
  // を用意するものなので、レコードとかは返却しません。実行するだけです
  //
  Napi::String _sql = info[0].As<Napi::String>();
  std::unique_ptr<SQLTCHAR> sql(OmniDb::NapiStringToSQLTCHAR(_sql));

  std::unique_ptr<SQLHSTMT, StmtAcc> stmt(StmtAcc::alloc(m_hOdbc));
  if (!SQL_SUCCEEDED(ret = SQLExecDirect(stmt.get(), sql.get(), SQL_NTS)))
  {
    CreateError(
        env,
        ErrorMessage(_O("SQLExecDirect"), ret, SQL_HANDLE_STMT, stmt.get()))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  return Napi::Boolean::New(env, true);
}

/**
 * SQLを直接実行し、レコードを返却します。ユーティリティとして使用することを目的とした簡易版です
 *
 * @param[in] info Node.jsパラメータ
 * @return Napi::Value SQLの実行結果をJSONで返します
 */
Napi::Value OmniDb::Records(const Napi::CallbackInfo &info)
{
  SQLRETURN ret;
  Napi::Env env = info.Env();

  //
  // records(sql)
  //
  // のパラメータチェック
  //
  if (info.Length() < 1)
  {
    CreateTypeError(
        env,
        OString(_O("records(sql) sqlパラメータは必須です")))
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  if (!info[0].IsString())
  {
    CreateTypeError(
        env,
        OString(_O("sql は文字列のみ指定できます")))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  //
  // SQL実行
  //
  Napi::String _sql = info[0].As<Napi::String>();
  std::unique_ptr<SQLTCHAR> sql(OmniDb::NapiStringToSQLTCHAR(_sql));
  std::unique_ptr<SQLHSTMT, StmtAcc> stmt(StmtAcc::alloc(m_hOdbc));
  if (!SQL_SUCCEEDED(ret = SQLExecDirect(stmt.get(), sql.get(), SQL_NTS)))
  {
    CreateError(
        env,
        ErrorMessage(_O("SQLExecDirect"), ret, SQL_HANDLE_STMT, stmt.get()))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  // 結果セットのカラム数を取得
  SQLSMALLINT numCols;
  if (!SQL_SUCCEEDED(ret = SQLNumResultCols(stmt.get(), &numCols)))
  {
    CreateError(
        env,
        ErrorMessage(_O("SQLNumResultCols"), ret, SQL_HANDLE_STMT, stmt.get()))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  //
  // カラム情報を設定
  //
  json columnIndex = json::object();
  std::vector<SQLSMALLINT> colTypes;
  for (SQLSMALLINT col = 1; col <= numCols; col++)
  {
    SQLTCHAR colName[255] = {0};
    SQLSMALLINT colType;
    SQLULEN colLength;

    if (!SQL_SUCCEEDED(ret = SQLDescribeCol(stmt.get(), col, colName, sizeof(colName), NULL, &colType, &colLength, NULL, NULL)))
    {
      CreateError(
          env,
          ErrorMessage(_O("SQLDescribeCol"), ret, SQL_HANDLE_STMT, stmt.get()))
          .ThrowAsJavaScriptException();
      return env.Null();
    }

    // カラム情報を保存
    columnIndex[to_jsonstr(_S2O(colName))] = col - 1;
    colTypes.push_back(colType);
  }

  //
  // レコード取得
  //
  json recs = json::array();

  while (SQLFetch(stmt.get()) == SQL_SUCCESS)
  {
    json rec = json::array();
    for (SQLSMALLINT col = 1; col <= numCols; col++)
    {
      // NULL値確認
      SQLLEN indicator;

// セットマクロ
#define SETDATA(t, buf, size, setv)                                                     \
  if (SQL_SUCCEEDED(ret = SQLGetData(stmt.get(), col, (t), (buf), (size), &indicator))) \
  {                                                                                     \
    if (indicator == SQL_NULL_DATA)                                                     \
      rec.push_back(nullptr);                                                           \
    else                                                                                \
      rec.push_back((setv));                                                            \
  }

      ret = SQL_SUCCESS;

      // データ設定 ※サポートしている
      switch (colTypes[col - 1])
      {
      case SQL_REAL:
      case SQL_DECIMAL:
      case SQL_NUMERIC:
      {
        SQLCHAR v[256] = {0};
        SETDATA(SQL_C_CHAR, v, sizeof(v), std::string((const char *)v));
        break;
      }
      case SQL_CHAR:
      case SQL_VARCHAR:
      case SQL_LONGVARCHAR:
      {
        SQLCHAR v[4096] = {0};
        SETDATA(SQL_C_CHAR, v, sizeof(v), std::string((const char *)v));
        break;
      }
      case SQL_WCHAR:
      case SQL_WVARCHAR:
      case SQL_WLONGVARCHAR:
      {
#ifdef UNICODE
        // Wide文字対応ドライバ ※windows
        SQLWCHAR v[2048] = {0};
        SETDATA(SQL_C_WCHAR, v, sizeof(v), wide_to_single(std::wstring((const wchar_t *)v)));
#else
        // その他ドライバの場合のWCHAR系はCHARとして扱う。そのためWCHARは現在はASCII以外はサポートしていない
        // ※対応する場合はiconv等で変換する必要がある。現時点ではないのでとりあえずこのまま
        SQLCHAR v[4096] = {0};
        SETDATA(SQL_C_CHAR, v, sizeof(v), std::string((const char *)v));
#endif
        break;
      }
      case SQL_FLOAT:
      case SQL_DOUBLE:
      {
        SQLDOUBLE v;
        SETDATA(SQL_C_DOUBLE, &v, sizeof(v), v);
        break;
      }
      case SQL_TINYINT:
      {
        SQLCHAR v;
        SETDATA(SQL_C_STINYINT, &v, sizeof(v), (SQLSMALLINT)v);
        break;
      }
      case SQL_SMALLINT:
      {
        SQLSMALLINT v;
        SETDATA(SQL_C_SHORT, &v, sizeof(v), v);
        break;
      }
      case SQL_INTEGER:
      {
        SQLINTEGER v = 0;
        SETDATA(SQL_C_SLONG, &v, sizeof(v), v);
        break;
      }
      case SQL_BIGINT:
      {
        SQLBIGINT v;
        // jsonがbitintサポートしていないのでとりあえずキャストで対応
        SETDATA(SQL_C_SBIGINT, &v, sizeof(v), (SQLINTEGER)v);
        break;
      }
      default:
      {
        // とりあえず文字列で返す
        SQLCHAR v[8192] = {0};
        SETDATA(SQL_C_CHAR, v, sizeof(v), std::string((const char *)v));
        break;
      }
      }

      if (!SQL_SUCCEEDED(ret))
      {
        CreateError(
            env,
            ErrorMessage(_O("SQLGetData"), ret, SQL_HANDLE_STMT, stmt.get()))
            .ThrowAsJavaScriptException();
        return env.Null();
      }
    }

    // レコード追加
    recs.push_back(rec);
  }

  json result = json::object();
  result["columnIndex"] = columnIndex;
  result["records"] = recs;
  return Napi::String::New(env, result.dump(-1, ' ', true, json::error_handler_t::replace));
}

/**
 * ロケール設定
 *
 * @param[in] info Node.jsパラメータ
 * @return Napi::Value カラム情報をJSON形式の文字列で返します
 */

Napi::Value OmniDb::SetLocale(const Napi::CallbackInfo &info)
{
  Napi::Env env = info.Env();

  // setLocale(category, locale)
  // のパラメータチェック
  if (info.Length() != 2)
  {
    CreateTypeError(
        env,
        OString(_O("setLocale(category, locale) category, localeパラメータは必須です")))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[0].IsString())
  {
    CreateTypeError(
        env,
        OString(_O("category は文字列のみ指定できます")))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[1].IsString())
  {
    CreateTypeError(
        env,
        OString(_O("locale は文字列のみ指定できます")))
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  Napi::String _category = info[0].As<Napi::String>();
  std::unique_ptr<SQLTCHAR> category(OmniDb::NapiStringToSQLTCHAR(_category));

  Napi::String _locale = info[1].As<Napi::String>();
  std::unique_ptr<SQLTCHAR> locale(OmniDb::NapiStringToSQLTCHAR(_locale));

  // カテゴリ名が一致した場合はロケール設定
  OString name = _S2O(category.get());
  int numType = sizeof(LOCALE_NAMES) / sizeof(LOCALE_NAME);

  for (int i = 0; i < numType; i++)
  {
    if (name.compare(_S2O(LOCALE_NAMES[i].name)) == 0)
    {
      osetlocale(LOCALE_NAMES[i].category, locale.get());
      break;
    }
  }
  return Napi::Boolean::New(env, true);
}

/**
 * ODBCエラー文字列取得
 */
OString OmniDb::ErrorMessage(const OString &api, SQLRETURN retcode, SQLSMALLINT handleType, SQLHANDLE hError)
{
  OString sqlmsg;

  //
  // SQLメッセージが取得できるやつだけ取得
  //
  if (retcode == SQL_ERROR)
  {
    // メッセージ数取得
    SQLLEN numRecs = 0;
    SQLGetDiagField(handleType, hError, 0, SQL_DIAG_NUMBER, &numRecs, 0, 0);

    // 全メッセージ取得
    SQLTCHAR state[32], odbcmsg[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT msgLen;
    SQLINTEGER native;
    SQLSMALLINT recNo = 1;
    while (
        recNo <= numRecs &&
        (SQLGetDiagRec(
             handleType, hError, recNo, state, &native,
             odbcmsg, sizeof(odbcmsg), &msgLen) != SQL_NO_DATA))
    {
      if (sqlmsg.length() > 0)
      {
        sqlmsg += _O(", ");
      }

      OString p = _O("[ODBC-ERROR]");
      p += _S2O(odbcmsg);
      p += _O("(API:");
      p += api;
      p += _O(", STATE:");
      p += _S2O(state);
      p += _O(", NATIVE:");
      p += to_ostring(native);
      p += _O(")");
      sqlmsg += p;
      recNo++;
    }
  }

  OString res;
  if (sqlmsg.length() > 0)
  {
    res += sqlmsg;
  }
  else
  {
    res += api + _O("エラー (CODE:") + to_ostring(retcode) + _O(")");
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
  for (int i = 0; i < numType; i++)
  {
    if (SQLTYPENAMES[i].type == type)
    {
      result = _S2O(SQLTYPENAMES[i].name);
      break;
    }
  }
  return result;
}

/**
 * SQL ODBC型名に対するクラス名を取得します
 *
 * @param[in] type ODBCの型
 * @return OString クラス名
 */
OString OmniDb::GetTypeClassName(SQLSMALLINT type)
{
  OString result = OString(_O(""));

  int numType = sizeof(SQLTYPENAMES) / sizeof(SQLTYPENAME);
  for (int i = 0; i < numType; i++)
  {
    if (SQLTYPENAMES[i].type == type)
    {
      result = _S2O(SQLTYPENAMES[i].className);
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
SQLTCHAR *OmniDb::NapiStringToSQLTCHAR(Napi::String string)
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
Napi::Object CreateObject(const Napi::CallbackInfo &info)
{
  return OmniDb::NewInstance(info.Env(), info);
}

/**
 * OmniDbライブラリを初期化します
 *
 * @param[in] env NAPIの環境
 * @param[in] exports エクポート関数
 * @return Napi::Object インスタンス
 */
Napi::Object InitAll(Napi::Env env, Napi::Object exports)
{
  Napi::Object new_exports = Napi::Function::New(env, CreateObject);
  return OmniDb::Init(env, new_exports);
}

NODE_API_MODULE(omnidb, InitAll)
