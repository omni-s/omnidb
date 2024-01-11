module.exports = {
  root: true,
  env: {
    node: true,
  },
  extends: ['plugin:vue/essential', 'eslint:recommended', '@vue/prettier'],
  parserOptions: {
    ecmaVersion: 2020,
  },
  rules: {
    'no-console': process.env.NODE_ENV === 'production' ? 'warn' : 'off',
    'no-debugger': process.env.NODE_ENV === 'production' ? 'warn' : 'off',
    // Vue コンポーネントの名前には複数のワード強要はオフ
    'vue/multi-word-component-names': 'off',
    // あえて未使用にしている _ の変数は未使用チェックしない
    'no-unused-vars': ['error', { argsIgnorePattern: '^_$' }],
  },
}
