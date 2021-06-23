#include <napi.h>
#include <time.h>
#include <stdlib.h>
#include <iostream>

#include "omnidb.h"
#include "nlohmann/json.hpp"

using json = nlohmann::json;

using namespace Napi;
uv_mutex_t OmniDb::g_odbcMutex;
SQLHENV OmniDb::g_hEnv;

// new() の定義
Napi::Object OmniDb::NewInstance(Napi::Env env, const Napi::CallbackInfo &info)
{
  Napi::EscapableHandleScope scope(env);
  const std::initializer_list<napi_value> initArgList = {info[0]};
  Napi::Object obj = env.GetInstanceData<Napi::FunctionReference>()->New(initArgList);
  return scope.Escape(napi_value(obj)).ToObject();
}

Napi::Object OmniDb::Init(Napi::Env env, Napi::Object exports)
{
  OmniDb::g_hEnv = NULL;
  OmniDb::g_odbcMutex = {0};

//  uv_mutex_init(&OmniDb::g_odbcMutex);

//  uv_mutex_lock(&OmniDb::g_odbcMutex);
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &OmniDb::g_hEnv);
//  uv_mutex_unlock(&OmniDb::g_odbcMutex);
  if (!SQL_SUCCEEDED(ret)) {
//    DEBUG_PRINTF("ODBC::New - ERROR ALLOCATING ENV HANDLE!!\n");
//    return env.Null();
// atode
    return exports;
  }
  SQLSetEnvAttr(OmniDb::g_hEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) SQL_OV_ODBC3, SQL_IS_UINTEGER);

  Napi::Function func = DefineClass(
    env, "omnidb", {
      InstanceMethod("connect", &OmniDb::Connect),
      InstanceMethod("drivers", &OmniDb::Drivers),
  });

  Napi::FunctionReference *constructor = new Napi::FunctionReference();
  *constructor = Napi::Persistent(func);
  env.SetInstanceData(constructor);

  exports.Set("omnidb", func);
  return exports;
}

OmniDb::OmniDb(const Napi::CallbackInfo &info) : Napi::ObjectWrap<OmniDb>(info)
{
  m_hOdbc = NULL;
};

OmniDb::~OmniDb(){};

Napi::Value OmniDb::Connect(const Napi::CallbackInfo &info)
{
  Napi::Env env = info.Env();

  if(info.Length() != 1) {
    Napi::TypeError::New(env, "connect(connectionString) requires 1 parameters.").ThrowAsJavaScriptException();
    return env.Null();
  }

  SQLTCHAR *connectionStringPtr = nullptr;

  if (info[0].IsString()) {
    Napi::String connectionString = info[0].As<Napi::String>();
    connectionStringPtr = OmniDb::NapiStringToSQLTCHAR(connectionString);
  } else {
    Napi::TypeError::New(env, "connect: A configuration object must have a 'connectionString' property that is a string.").ThrowAsJavaScriptException();
    return env.Null();
  }


  // DB接続
  // https://www.ibm.com/docs/ja/i/7.3?topic=details-connection-string-keywords
  SQLAllocHandle(SQL_HANDLE_DBC, OmniDb::g_hEnv, &m_hOdbc);
  if(!SQL_SUCCEEDED(SQLDriverConnect(m_hOdbc, NULL, connectionStringPtr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE))) {
    Napi::Error::New(env, "hoge").ThrowAsJavaScriptException();
    return env.Null();
  }

  return Napi::Boolean::New(env, true);
//  return Napi::Number::New(env, 1.0);
}

Napi::Value OmniDb::Drivers(const Napi::CallbackInfo &info)
{
  Napi::Env env = info.Env();
  std::cout << g_hEnv << "drivers\n";
  json drivers = json::array();

	SQLTCHAR _driver[256];
	SQLTCHAR _attribute[512];

	SQLSMALLINT dret, aret;
  SQLRETURN ret;

  while(
    SQL_SUCCEEDED(ret = SQLDrivers(
      OmniDb::g_hEnv, (SQLUSMALLINT)SQL_FETCH_FIRST,
      _driver, sizeof(_driver), &dret,
      _attribute, sizeof(_attribute), &aret))) {
// うまく動かない
    json driver = json::object();
    std::cout << ret << _driver << "\n";

    driver["name"] = _driver;
    driver["attribute"] = _attribute;
    drivers.push_back(driver);
  }

  return Napi::String::New(env, drivers.dump());
}

// Take a Napi::String, and convert it to an SQLTCHAR*, which maps to:
//    UNICODE : SQLWCHAR*
// no UNICODE : SQLCHAR*
SQLTCHAR* OmniDb::NapiStringToSQLTCHAR(Napi::String string) {

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


Napi::Object CreateObject(const Napi::CallbackInfo& info) {
  return OmniDb::NewInstance(info.Env(), info);
}

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  Napi::Object new_exports = Napi::Function::New(env, CreateObject);
  return OmniDb::Init(env, new_exports);
}

NODE_API_MODULE(omnidb, InitAll)
