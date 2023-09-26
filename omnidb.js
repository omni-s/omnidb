const OmniDbNative = require('bindings')('omnidb');

const { isPostgres, setPostgresTables, setPostgresColumns } = require('./postgres.js');

class OmniDb {
  constructor() {
    this._native = new OmniDbNative();
    this._dbms = '';
  }
  drivers() {
    return new Promise((resolve) => {
      resolve(JSON.parse(this._native.drivers()));
    });
  }
  connect(connectionString) {
    return new Promise((resolve) => {
      const result = this._native.connect(connectionString)
      if(result) {
        this._dbms = this._native.dbms();
      }
      resolve(result);
    });
  }
  disconnect() {
    return new Promise((resolve) => {
      const result = this._native.disconnect();
      if(result) {
        this._dbms = '';
      }
      resolve(result);
    });
  }
  dbms() {
    return this._dbms
  }
  schemas(condition = {}) {
    return new Promise((resolve) => {
      // 全て取得するようにする
      condition.schema = '%';
      condition.tableType = '%';
      // テーブル一覧からスキーマのみ抽出する ※重複はカット
      const schema = JSON.parse(this._native.tables(condition)).map(item => item.schema)
      resolve([...new Set(schema)]);
    });
  }
  tables(condition) {
    return new Promise((resolve) => {
      const tables = JSON.parse(this._native.tables(condition));
      if(isPostgres(this.dbms())) {
        // PostgreSQLはテーブル名がODBCから取得できないためSQLで取得する
        setPostgresTables(this, tables).then((t) => {
          resolve(t);
        });
      } else {
        resolve(tables);
      }
    });
  }
  columns(condition) {
    return new Promise((resolve) => {
      const columns = JSON.parse(this._native.columns(condition));
      if(isPostgres(this.dbms())) {
        // PostgreSQLはカラムのRemarkがcolumnsからは取得できないので、SQLで取得する
        setPostgresColumns(this, columns).then((c) => {
          resolve(c);
        });
      } else {
        resolve(columns);
      }
    });
  }
  primaryKeys(condition = {}) {
    return new Promise((resolve) => {
      // 主キー情報を取得する
      const keys = JSON.parse(this._native.primaryKeys(condition));
      resolve(keys);
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
  records(sql) {
    return new Promise((resolve) => {
      resolve(JSON.parse(this._native.records(sql)));
    });
  }
  setLocale(category, locale) {
    return this._native.setLocale(category, locale);
  }
}


module.exports = OmniDb;
