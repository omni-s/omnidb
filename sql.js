/**
 * SQLの文字列をエスケープします。
 * @param {string} inputString SQLをエスケープする文字列
 * @returns {string} エスケープされたSQL文字列
 */
const escapeSqlString = (inputString) => {
  // シングルクォートをエスケープ
  let escapedString = inputString.replace(/'/g, "''")

  // 他の特殊文字をエスケープ（必要に応じて他の特殊文字も追加してください）
  escapedString = escapedString.replace(/\\/g, '\\\\') // バックスラッシュ
  escapedString = escapedString.replace(/"/g, '\\"') // ダブルクォート

  return escapedString
}

exports.escapeSqlString = escapeSqlString
