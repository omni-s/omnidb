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
  return inputString.replace(/[\r\n\t]|\r\n/g, ' ');
}
exports.replaceSpecialChars = replaceSpecialChars
