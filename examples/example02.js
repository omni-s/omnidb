const omnidb = require('../omnidb');
const db = new omnidb();
db.connect('dsn=phpq;uid=qsecofr;pwd=passw0rd;Database=C7054D00;CCSID=1208;EXTCOLINFO=1;')
.then((r) => {
  console.log('connect');
  console.log(r);
  return db.query('失敗くえり select * from DEMQUERY.DEMSHN where SNURTN >= ? and SNKGZK >= ? and SNSHNM= ?');
})
.then((r) => {
  console.log('query');
  console.log(r);
})
.catch((e) => {
  console.log('エラー発生');
  console.log(e);
});

