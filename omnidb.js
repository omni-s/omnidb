const OmniDbNative = require('bindings')('omnidb');

const {
  isAS400, getAS400Schemas, getAS400CurrentSchema
} = require('./as400.js');

const {
  isPostgres,
  setPostgresTables,
  setPostgresColumns,
  getPostgresCurrentSchema
} = require('./postgres.js');

const { 
  isMySQL,
  getMySQLSchemas,
  getMySQLTables,
  getMySQLColumns,
  getMySQLPrimaryKeys,
  getMySQLQuery,
  getMySQLCurrentSchema
} = require('./mysql.js');

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
      if(isAS400(this.dbms())) {
        // AS400はODBCのSQLTablesではうまく取得できないのでSQLで取得する
        getAS400Schemas(this).then((schemas) => {
          resolve(schemas);
        });
      } else if(isMySQL(this.dbms())) {
        // MySQLはスキーマ名がODBCから取得できないためSQLで取得する
        getMySQLSchemas(this).then((schemas) => {
          resolve(schemas);
        });
      } else {
        // 全て取得するようにする
        condition.schema = '%';
        condition.tableType = '%';
        // テーブル一覧からスキーマのみ抽出する ※重複はカット
        const _tmp = JSON.parse(this._native.tables(condition)).map(item => `${item.catalog}|${item.schema}`)
        const schemas = [...new Set(_tmp)].map(t => {
          const [catalog, schema] = t.split('|');
          return {
            catalog: catalog,
            name: schema,
            remarks: '',
          }
        })
        resolve(schemas);
      }
    });
  }
  currentSchema() {
    // カレントスキーマの取得はODBC経由では行えないのでSQLで取得する
    return new Promise((resolve) => {
      if(isAS400(this.dbms())) {
        getAS400CurrentSchema(this).then((schema) => {
          resolve(schema);
        });
      } else if(isMySQL(this.dbms())) {
        getMySQLCurrentSchema(this).then((schema) => {
          resolve(schema);
        });
      } else if(isPostgres(this.dbms())) {
        getPostgresCurrentSchema(this).then((schema) => {
          resolve(schema);
        });
      } else {
        // その他の場合はカレントスキーマは空文字を返す
        resolve('');
      }
    });
  }
  tables(condition) {
    return new Promise((resolve) => {
      let tables = JSON.parse(this._native.tables(condition));
      if(isPostgres(this.dbms())) {
        // PostgreSQLはテーブル名がODBCから取得できないためSQLで取得する
        setPostgresTables(this, tables).then((t) => {
          resolve(t);
        });
      } else {
        if(isMySQL(this.dbms())) {
          tables = getMySQLTables(tables);
        }
        resolve(tables);
      }
    });
  }
  columns(condition) {
    return new Promise((resolve) => {
      let columns = JSON.parse(this._native.columns(condition));
      if(isPostgres(this.dbms())) {
        // PostgreSQLはカラムのRemarkがcolumnsからは取得できないので、SQLで取得する
        setPostgresColumns(this, columns).then((c) => {
          resolve(c);
        });
      } else {
        if(isMySQL(this.dbms())) {
          columns = getMySQLColumns(columns);
        }
        resolve(columns);
      }
    });
  }
  primaryKeys(condition = {}) {
    return new Promise((resolve) => {
      // 主キー情報を取得する
      let keys = JSON.parse(this._native.primaryKeys(condition));
      if(isMySQL(this.dbms())) {
        keys = getMySQLPrimaryKeys(keys);
      }
      resolve(keys);
    });
  }
  query(queryString, options) {
    return new Promise((resolve) => {
      let result = JSON.parse(this._native.query(queryString, options));
      if(isMySQL(this.dbms())) {
        result = getMySQLQuery(result);
      }
      resolve(result);
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
