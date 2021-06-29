{
  "targets": [
    {
      "target_name": "omnidb",
      "cflags!": [ "-fno-exceptions .source-charset:utf-8" ],
      "cflags_cc!": [ "-fno-exceptions /source-charset:utf-8" ],
      'cflags' : ['-Wall', '-Wextra', '-Wno-unused-parameter', '-DNAPI_DISABLE_CPP_EXCEPTIONS'],
      "sources": [ "src/omnidb.cpp" ],
      "include_dirs": [
        "<!@(node -p \"require('node-addon-api').include\")"
      ],
      "defines": [ 'NAPI_DISABLE_CPP_EXCEPTIONS' ],
      'conditions' : [
        [ 'OS == "linux"', {
          'libraries' : [
            '-lodbc'
          ]
        }],
        [ 'OS == "mac"', {
          'include_dirs': [
            '/usr/local/include'
          ],
          'libraries' : [
            '-L/usr/local/lib',
            '-lodbc'
          ]
        }],
        [ 'OS=="win"', {
          'libraries' : [
            '-lodbccp32.lib'
          ],
          'defines': [ 'NAPI_DISABLE_CPP_EXCEPTIONS', 'UNICODE' ]
        }],
        [ 'OS=="aix"', {
          'variables': {
            'os_name': '<!(uname -s)',
          },
          'conditions': [
            [ '"<(os_name)"=="OS400"', {
              'ldflags': [
                  '-Wl,-brtl,-blibpath:/QOpenSys/pkgs/lib,-lodbc'
              ],
              'cflags' : ['-std=c++0x', '-DNAPI_DISABLE_CPP_EXCEPTIONS', '-Wall', '-Wextra', '-Wno-unused-parameter', '-I/QOpenSys/usr/include', '-I/QOpenSys/pkgs/include']
            }]
          ]
        }]
      ]
    }
  ]
}