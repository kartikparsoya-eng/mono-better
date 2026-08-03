{
  'includes': ['deps/common.gypi'],
  'targets': [
    {
      'target_name': 'better_sqlite3',
      'sources': ['src/better_sqlite3.cpp'],
      'include_dirs': ['/usr/local/include'],
      'libraries': ['-L/usr/local/lib', '-lsqlite3'],
      'cflags_cc': ['-std=c++20'],
      'conditions': [
        ['OS=="linux"', {
          'ldflags': ['-Wl,-rpath,/usr/local/lib'],
        }],
      ],
    },
  ],
}
