const OmniDbNative = require('bindings')('omnidb');
class OmniDb {
  constructor() {
    this._native = new OmniDbNative();
  }
  drivers() {
    return new Promise((resolve) => {
      resolve(JSON.parse(this._native.drivers()));
    });
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
  query(queryString, options) {
    return new Promise((resolve) => {
      resolve(JSON.parse(this._native.query(queryString, options)));
    });
  }
  execute(sql) {
    return new Promise((resolve) => {
      resolve(JSON.parse(this._native.execute(sql)));
    });
  }
  setLocale(category, locale) {
    return this._native.setLocale(category, locale);
  }
}

module.exports = OmniDb;
