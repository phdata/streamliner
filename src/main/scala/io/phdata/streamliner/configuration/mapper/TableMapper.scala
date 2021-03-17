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

}
