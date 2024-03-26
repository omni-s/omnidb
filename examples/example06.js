const omnidb = require('../omnidb')

const db = new omnidb()
;(async () => {
  console.log('// setlocale')
  console.log(db.setLocale('LC_ALL', ''));

  console.log('// drivers')
  console.log(await db.drivers());

  console.log('// connect')
  console.log(await db.connect('dsn=oracle;UID=apibridge;PWD=apibridge00;Dbq=ec2-54-95-133-101.ap-northeast-1.compute.amazonaws.com:1521/XEPDB1'))

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
  console.log(await db.tables({ schema: 'TEST' }))
  console.log('// tables2')
  console.log(await db.tables({ schema: 'APIBRIDGE' }))
  console.log('// columns')
  console.log(await db.columns({ schema: 'TEST', table: 'TEST_TABLE_1' }))
  console.log('// columns2')
  console.log(await db.columns({ table: 'TestTypes' }))
  console.log('// columns3')
  console.log(await db.columns({ table: '日本語テーブル' }))
  console.log('// primary keys')
  console.log(await db.primaryKeys({ schema: 'TEST', table: 'TEST_TABLE_1' }))
  console.log('// query')
  console.log(await db.query('select * from "account" where "id" <= ?'));
  console.log('// query2')
  console.log(await db.query('SELECT A.*, B.* FROM TEST.TEST_TABLE_1 A, TEST.TEST_TABLE_2 B WHERE A.id >= ?'))
  console.log('// query3')
  console.log(await db.query('SELECT * FROM "日本語テーブル"'))
  // console.log('// BAD query');
  // try {
  //   console.log(await db.query("SELECT FOO, BAR FROM QGPL.DEMSHN"));
  // } catch(e) {
  //   console.error(e)
  // }
  console.log('// records')
  console.log(await db.records('select * from "account" where "id" >= 9000'))

  console.log('// disconnect')
  console.log(await db.disconnect())
})()
