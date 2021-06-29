const OmniDb = require('bindings')('omnidb');

const obj = new OmniDb();

console.log('// connect');
console.log(obj.connect(`dsn=i4;uid=qsecofr;pwd=passw0rd;Database=C7054D00;CCSID=1208;EXTCOLINFO=1;`));

console.log('// tables');
console.log(JSON.parse(obj.tables({schema: 'DEMQUERY'})));
console.log('// columns');
console.log(JSON.parse(obj.columns({schema: 'DEMQUERY', table:'DEMSHN'})));
console.log('// query');
console.log(JSON.parse(obj.query("select * from DEMQUERY.DEMSHN where SNURTN >= ? and SNKGZK >= ? and SNSHNM= ?")));
console.log('// disconnect');
console.log(obj.disconnect());

//console.log(obj.drivers());

