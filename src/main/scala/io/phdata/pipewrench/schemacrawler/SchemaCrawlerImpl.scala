package io.phdata.pipewrench.schemacrawler

import java.sql.Connection

import io.phdata.pipewrench.configuration.Jdbc
import schemacrawler.schema.Catalog
import schemacrawler.schemacrawler.{DatabaseConnectionOptions, RegularExpressionInclusionRule, SchemaCrawlerOptionsBuilder, SchemaInfoLevelBuilder}
import schemacrawler.utility.SchemaCrawlerUtility

object SchemaCrawlerImpl {

  def getCatalog(jdbc: Jdbc): Catalog = {
    val options = SchemaCrawlerOptionsBuilder.builder()
      .withSchemaInfoLevel(SchemaInfoLevelBuilder.standard())
      .includeSchemas(new RegularExpressionInclusionRule(jdbc.schema))
      .toOptions

    SchemaCrawlerUtility.getCatalog(getConnection(jdbc), options)
  }

  private def getConnection(jdbc: Jdbc): Connection = {
    val con = new DatabaseConnectionOptions(jdbc.url)
    con.getConnection(jdbc.username, jdbc.password)
  }

}
