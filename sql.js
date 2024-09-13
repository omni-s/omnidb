/**
 * SQLの文字列をエスケープします。
 * @param {string} inputString SQLをエスケープする文字列
 * @param {boolean} isBackslash バックスラッシュエスケープを使うか
 * @returns {string} エスケープされたSQL文字列
 */
const escapeSqlString = (inputString, isBackslash = true) => {
  // シングルクォートをエスケープ
  let escapedString = inputString.replace(/'/g, "''")

  if (isBackslash) {
    // 他の特殊文字をエスケープ（必要に応じて他の特殊文字も追加してください）
    escapedString = escapedString.replace(/\\/g, '\\\\') // バックスラッシュ
    escapedString = escapedString.replace(/"/g, '\\"') // ダブルクォート
  }

  return escapedString
}
exports.escapeSqlString = escapeSqlString

/**
 * 特殊文字のスペース置換
 * @param {string} inputString SQL文字列
 * @returns {string}
 */
const replaceSpecialChars = (inputString) => {
  // 改行、タブをスペースに置き換え
  return inputString.replace(/[\r\n\t]|\r\n/g, ' ')
}
exports.replaceSpecialChars = replaceSpecialChars

/**
 * 全角半角スペースを削除する
 * @param {string} str 文字列
 * @returns {string} 全角半角スペースを削除した文字列
 */
const trimSpaces = (str) => {
  if (!str) return str

  return str.replace(/^[ \u3000]+|[ \u3000]+$/g, '')
}
exports.trimSpaces = trimSpaces

/**
 * SELECT文かどうかを判定します。
 * @param {string} sql チェックするSQL
 * @returns {boolean} SELECT文かどうか
 */
const isSelectQuery = (sql) => {
  // コメント除去してチェック
  const _sql = normalizedQuery(sql)
  return /^\s*SELECT\b/i.test(_sql)
}
exports.isSelectQuery = isSelectQuery

/**
 * SQLを正規化します。
 * @param {string} sql SQL
 * @returns {string} 正規化されたSQL
 */
const normalizedQuery = (sql) => {
  // SQLのコメントを削除する
  return sql.replace(/\/\*[\s\S]*?\*\/|--.*$/gm, '').trim()
}
exports.normalizedQuery = normalizedQuery
