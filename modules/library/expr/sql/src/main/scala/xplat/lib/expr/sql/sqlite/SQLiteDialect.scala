package xplat.lib.expr.sql.sqlite

import xplat.lib.expr.sql.base.Select

trait SQLiteDialect extends
  Select.Filter with
  Select.Map with
  Select.Operations with
  Select.FlatMap with
  Select.JoinOverTable

object SQLiteDialect extends SQLiteDialect {}
