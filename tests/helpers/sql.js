const canonicalizeSQL = (sql) =>
  sql
    .replace(/\s+/g, ' ')
    .replace(/\s*\(\s*/g, '(')
    .replace(/\s*\)\s*/g, ')')
    .replace(/"{3}/g, '"')
    .trim()
    .toLowerCase();

module.exports = { canonicalizeSQL };
