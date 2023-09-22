const omnidb = require('../omnidb');
const db = new omnidb();
(async () => {
  console.log('// setlocale');
  console.log(db.setLocale('LC_ALL', ''));

  console.log('// drivers');
  console.log(await db.drivers());

  console.log('// connect');
  console.log(await db.connect('dsn=phpq;uid=qsecofr;pwd=passw0rd;Database=C7054D00;CCSID=1208;EXTCOLINFO=1;'));
  console.log('// dbms');
  console.log(await db.dbms())
  console.log('// schemas');
  console.log(await db.schemas());
  console.log('// tables');
  console.log(await db.tables({schema: 'DEMQUERY'}));
  console.log('// columns');
  console.log(await db.columns({schema: 'DEMQUERY', table:'DEMSHN'}));
  console.log('// query');
  console.log(await db.query('select * from DEMQUERY.DEMSHN where SNURTN >= ? and SNKGZK >= ? and SNSHNM= ?'));
  console.log('// BAD query');
  try {
    console.log(await db.query("SELECT FOO, BAR FROM QGPL.DEMSHN"));
  } catch(e) {
    console.error(e)
  }

  // execute check
  console.log('// execute');
  try {
    console.log('//-- SET CURRENT SCHEMA')
    console.log(await db.execute('SET CURRENT SCHEMA=\'PHPQUERY2\''));
    console.log('//-- CHECK')
    console.log(await db.query('SELECT DRKMTN, DRKFID, DRKFLID, DRDMTN, DRDFNM FROM DB2DRGS WHERE DRKMTN LIKE ?'));
  } catch(e) {
    console.error(e);
  }

  console.log('// disconnect');
  console.log(await db.disconnect());
})();

