#ifndef _OMNIDB_H
#define _OMNIDB_H
#include <uv.h>
#include <napi.h>
#include <wchar.h>

#include <algorithm>

#include <stdlib.h>
#include <sql.h>
#include <sqlext.h>
#include <locale.h>

// Convert a wide Unicode string to an UTF8 string

#ifdef UNICODE
  //
  // Windows用(WideChar対応)
  //

  // OmniDb文字列型
  typedef std::wstring OString;
  // OmniDb文字列ストリーム
  typedef std::wstringstream OStringStream;
  // 標準出力
  #define ocout std::wcout
  // locale設定
  #define osetlocale(c, l) (_wsetlocale((c), (const wchar_t *)(l)))
  // 文字列コピー
  #define ostrcpy(s1,s2) (wcscpy((wchar_t *)(s1), (const wchar_t *)(s2)))
  // 数値文字列変換
  #define to_ostring(s) (std::to_wstring((s)))
  // 文字列リテラル
  #define _O(s) ((const wchar_t *)(L##s))
  #define _S2O(s) (OString((const wchar_t *)(s)))
  // ネイティブ文字列
  #define _N(s) ((char16_t *)(s))
  
  // JSON文字列変換(utf-8に変換)
  std::string to_jsonstr(const std::wstring &wstr)
  {
    // utf-8専用。windowsだと切り替えないと駄目
    if( wstr.empty() ) return std::string();
    int size_needed = WideCharToMultiByte(CP_UTF8, 0, &wstr[0], (int)wstr.size(), NULL, 0, NULL, NULL);
    std::string strTo( size_needed, 0 );
    WideCharToMultiByte(CP_UTF8, 0, &wstr[0], (int)wstr.size(), &strTo[0], size_needed, NULL, NULL);
    return strTo;
  }
#else
  //
  // UNIX用(utf-8ベース)
  //
  typedef std::string OString;
  // OmniDb文字列ストリーム
  typedef std::stringstream OStringStream;
  // 標準出力
  #define ocout std::cout
  // locale設定
  #define osetlocale(c, l) (setlocale((c), (const char *)(l)))
  // 文字列コピー
  #define ostrcpy(s1,s2) (strcpy((char *)(s1), (const char *)(s2)))
  // O数値文字列変換
  #define to_ostring(s) (std::to_string((s)))
  // 文字列リテラル
  #define _O(s) ((const char *)(s))
  #define _S2O(s) OString((const char *)(s))
  // ネイティブ文字列
  #define _N(s) ((char *)(s))
  // JSON文字列変換(utf-8に変換) ※何もしない
  #define to_jsonstr(s) ((const char *)(s))
#endif

class OmniDb : public Napi::ObjectWrap<OmniDb> {
public:

  // 初期化
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  // OmniDb作成
  static Napi::Object NewInstance(Napi::Env env, const Napi::CallbackInfo& info);

  OmniDb(const Napi::CallbackInfo& info);
  ~OmniDb() override;

  // DB接続
  Napi::Value Connect(const Napi::CallbackInfo& info);
  // DB切断
  Napi::Value Disconnect(const Napi::CallbackInfo& info);
  // ドライバ情報取得
  Napi::Value Drivers(const Napi::CallbackInfo& info);
  // DBMS名取得
  Napi::Value Dbms(const Napi::CallbackInfo &info);
  // テーブル情報取得
  Napi::Value Tables(const Napi::CallbackInfo& info);
  // カラム情報取得
  Napi::Value Columns(const Napi::CallbackInfo& info);
  // 主キー情報取得
  Napi::Value PrimaryKeys(const Napi::CallbackInfo& info);
  // SQL情報取得
  Napi::Value Query(const Napi::CallbackInfo& info);
  // SQL直接実行 ※成否のみ返却
  Napi::Value Execute(const Napi::CallbackInfo& info);
  // レコード取得(SQL実行)
  Napi::Value Records(const Napi::CallbackInfo& info);
  // ロケール設定
  Napi::Value SetLocale(const Napi::CallbackInfo& info);
private:
  // 接続ハンドル
  SQLHDBC m_hOdbc;
  // ODBC環境
  SQLHENV m_hEnv;

  // DB切断
  void _Disconnect();

  // ODBCエラーメッセージ取得
  OString ErrorMessage(const OString &api, SQLRETURN retcode, SQLSMALLINT handleType, SQLHANDLE hError);

  // SQL型名取得
  static OString GetTypeName(SQLSMALLINT type);
  // SQL型属性
  static OString GetTypeClassName(SQLSMALLINT type);

  // 型エラー作成(NAPI)
  static Napi::TypeError CreateTypeError(napi_env env, const OString &msg);
  // エラー作成(NAPI)
  static Napi::Error CreateError(napi_env env, const OString &msg);

  // 空文字列判定
  static bool IsBlank(Napi::String v);

  // NAPI文字列→SQLCHAR変換
  static SQLTCHAR* NapiStringToSQLTCHAR(Napi::String string);

  // 左空白削除
  static OString leftTrim(const OString& str)
  {
    OString res = str;
    res.erase(0, res.find_first_not_of(_O(" ")));
    return res;
  }

  // 右空白削除
  static OString rightTrim(const OString& str)
  {
    OString res = str;
    res.erase(res.find_last_not_of(_O(" ")) + 1);
    return res;
  }

  // 前後空白削除
  static OString trimString(const OString& str)
  {
    return leftTrim(rightTrim(str));
  }
};

#endif
