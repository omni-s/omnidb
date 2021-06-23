const OmniDb = require('bindings')('omnidb');

const obj = new OmniDb();

console.log(obj.connect('dsn=i4;uid=qsecofr;pwd=passw0rd'));
//console.log(obj.drivers());

