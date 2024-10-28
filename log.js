const crypto = require('crypto')

// シグネチャ
const signature = 'omnidb'

/**
 * デバッグモードかどうか
 */
exports.isDebug = process.env.DEBUG_OMNIDB ? true : false

/**
 * ログ取得関数
 */
exports.logger = (...args) => {
  console.log(...args)
}

/**
 * ログに日時を含めるかどうか
 */
exports.includeTimestamp = true

/**
 * ログ用のIDを生成します。in/outなどでログを紐付けるために使用します。
 * @returns {string} ログ用のID
 */
exports.genLogId = () => {
  return crypto.randomBytes(4).toString('hex')
}

/**
 * ログメッセージを整形します。
 * @param {string} s ログメッセージ
 * @returns {string} 整形されたログメッセージ
 */
exports.getLogMsg = (s) => {
  if (!s) {
    return ''
  }

  // ログだと改行があると見ずらいので、改行をスペースに変換する
  // 行の先頭と末尾のスペースも削除
  const lines = s.split(/\r?\n/)
  const trimmedLines = lines.map((line) => line.trim())
  const result = trimmedLines.join(' ').trim()
  return result
}

/**
 * デバッグログを出力します。
 * @param  {...any} args デバッグログに出力する内容
 */
exports.debugLog = (...args) => {
  if (this.isDebug) {
    if (this.includeTimestamp) {
      const now = new Date()
      const time = now.toLocaleTimeString() + '.' + String(now.getMilliseconds()).padStart(3, '0')
      this.logger(`[${signature}] ${time} :`, ...args)
    } else {
      this.logger(`[${signature}]`, ...args)
    }
  }
}
