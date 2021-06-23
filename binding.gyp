{
  "targets": [
    {
      "target_name": "omnidb",
      "cflags!": [ "-fno-exceptions .source-charset:utf-8" ],
      "cflags_cc!": [ "-fno-exceptions /source-charset:utf-8" ],
      "sources": [ "src/omnidb.cpp" ],
      "include_dirs": [
        "<!@(node -p \"require('node-addon-api').include\")"
      ],
      "defines": [ 'NAPI_DISABLE_CPP_EXCEPTIONS', 'UNICODE' ],
      'libraries' : [
        '-lodbccp32.lib'
      ],
    }
  ]
}