package io.phdata.streamliner.util

import io.phdata.streamliner.configuration.ColumnDefinition
import org.scalatest.FunSuite

class TemplateFunctionTest extends FunSuite {

  test("normalize identity column definition") {
    val col = ColumnDefinition("sqlserver", "hive", "int identity")
    val cleanCol = TemplateFunction.normalizeColumnDefinition(col)
    assert("int" == cleanCol.dataType)
  }
}
