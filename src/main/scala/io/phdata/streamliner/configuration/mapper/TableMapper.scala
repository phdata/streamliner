package io.phdata.streamliner.configuration.mapper

import com.amazonaws.services.glue.model.{Table => AWSTable}
import io.phdata.streamliner.configuration.TableDefinition
import io.phdata.streamliner.configuration.UserDefinedTable
import schemacrawler.schema.{Table => SchemaCrawlerTable}

trait TableMapper {

  def mapSchemaCrawlerTables(
      tables: List[SchemaCrawlerTable],
      userDefinedTables: Option[Seq[UserDefinedTable]]): Seq[TableDefinition]

  def mapAWSGlueTables(
      tables: List[AWSTable],
      userDefinedTable: Option[Seq[UserDefinedTable]]): Seq[TableDefinition]

  def cleanseName(s: String): String = {
    val specialCharRegex = "(/|-|\\(|\\)|\\s|\\$)".r
    val specialChars = specialCharRegex.replaceAllIn(s.toLowerCase, "_")
    val dupsRegex = "(_{2,})".r
    val dups = dupsRegex.replaceAllIn(specialChars, "_")

    if (dups.startsWith("/") || dups.startsWith("_")) {
      dups.substring(1, dups.length)
    } else {
      dups
    }
  }

}
