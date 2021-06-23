#ifndef _OMNIDB_H
#define _OMNIDB_H
#include <uv.h>
#include <napi.h>
#include <wchar.h>

#include <algorithm>

#include <stdlib.h>
#include <sql.h>
#include <sqlext.h>


class OmniDb : public Napi::ObjectWrap<OmniDb> {
public:
    static uv_mutex_t g_odbcMutex;
    static SQLHENV g_hEnv;

    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    static Napi::Object NewInstance(Napi::Env env, const Napi::CallbackInfo& info);

    OmniDb(const Napi::CallbackInfo& info);
    ~OmniDb();

    Napi::Value Connect(const Napi::CallbackInfo& info);
    Napi::Value Drivers(const Napi::CallbackInfo& info);
//    Napi::Value QueryInformation(const Napi::CallbackInfo& info);
//    Napi::Value Tables(const Napi::CallbackInfo& info);
private:
    static SQLTCHAR* NapiStringToSQLTCHAR(Napi::String string);

    SQLHDBC m_hOdbc;
};

#endif
