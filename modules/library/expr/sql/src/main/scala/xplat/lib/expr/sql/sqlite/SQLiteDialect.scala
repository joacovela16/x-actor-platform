package xplat.lib.expr.sql.sqlite

import xplat.lib.expr.sql.base.{Insert, Select}

trait SQLiteDialect extends
  Select.Filter with
  Select.Map with
  Select.Operations with
  Select.FlatMap with
  Select.JoinOverTable with
  Insert.InsertEnabled with
  Insert.CommonInsert

object SQLiteDialect extends SQLiteDialect {}
