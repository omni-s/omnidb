const OmniDbNative = require('bindings')('omnidb');
class OmniDb {
  constructor() {
    this._native = new OmniDbNative();
  }
  connect(connectionString) {
    return new Promise((resolve) => {
      resolve(this._native.connect(connectionString));
    });
  }
  disconnect() {
    return new Promise((resolve) => {
      resolve(this._native.disconnect());
    });
  }
  tables(condition) {
    return new Promise((resolve) => {
      resolve(JSON.parse(this._native.tables(condition)));
    });
  }
  columns(condition) {
    return new Promise((resolve) => {
      resolve(JSON.parse(this._native.columns(condition)));
    });
  }
  query(queryString) {
    return new Promise((resolve) => {
      resolve(JSON.parse(this._native.query(queryString)));
    });
  }
}



module.exports = OmniDb;
