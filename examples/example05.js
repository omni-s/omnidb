const omnidb = require('../omnidb')
const db = new omnidb()
;(async () => {
  console.log('// setlocale')
  //console.log(db.setLocale('LC_ALL', ''));

  console.log('// drivers')
  //console.log(await db.drivers());

  console.log('// connect')
  console.log(await db.connect('dsn=mssql-ftds;UID=apibridge;PWD=Api-Bridge-00;Database=apibridge'))
  //	return;
  console.log('// dbms')
  console.log(db.dbms())
  console.log('// driver')
  console.log(db.driver())
  console.log('// schemas')
  console.log(await db.schemas())
  console.log('// current schema')
  console.log(await db.currentSchema())
  console.log('// tables')
  //console.log(await db.tables({ schema: 'dbo' }))
  console.log(await db.tables({ schema: 'test' }))
  console.log('// columns')
  console.log(await db.columns({ schema: 'test', table: 'test_table_1' }))
  console.log('// columns2')
  console.log(await db.columns({ table: 'TestTypes' }))
  console.log('// primary keys')
  console.log(await db.primaryKeys({ schema: 'test', table: 'test_table_1' }))
  console.log('// query')
  //console.log(await db.query('select * from \"account\" where id <= ?'));
  console.log(await db.query('select * from [test].[test_table_1] where id <= ?'))
  // console.log('// BAD query');
  // try {
  //   console.log(await db.query("SELECT FOO, BAR FROM QGPL.DEMSHN"));
  // } catch(e) {
  //   console.error(e)
  // }
  console.log('// records')
  console.log(await db.records('select * from [account] where id <= 10'))
  //console.log(await db.records('select CAST(1.5 AS bigint) AS A, CAST(2.22 AS int) AS B, CAST(3.1 AS smallint) AS C'));

  console.log('// disconnect')
  console.log(await db.disconnect())
})()
