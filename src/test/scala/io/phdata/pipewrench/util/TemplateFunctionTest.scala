package io.phdata.pipewrench.util

import io.phdata.pipewrench.configuration.ColumnDefinition
import org.scalatest.FunSuite

class TemplateFunctionTest extends FunSuite {

  test("normalize identity column definition") {
    val col = ColumnDefinition("sqlserver", "hive", "int identity")
    val cleanCol = TemplateFunction.normalizeColumnDefinition(col)
    assert("int" == cleanCol.dataType)
  }
}
