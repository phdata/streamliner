package io.phdata.pipewrench

package object configuration {

  case class Configuration(
      name: String,
      environment: String,
      pipeline: String,
      jdbc: Jdbc,
      hadoop: Hadoop)

  case class Jdbc(
      driverClass: Option[String],
      url: String,
      username: String,
      password: String,
      schema: String,
      tables: Option[Seq[Table]],
      metadata: Option[Map[String, String]])

  case class Hadoop(
      username: String,
      password: String,
      impalaShellCommand: String,
      stagingDatabase: Database,
      reportingDatabase: Database)

  case class Database(name: String, path: String)

  case class Table(
      name: String,
      checkColumn: Option[String] = None,
      numberOfMappers: Option[Int] = Some(1),
      splitByColumn: Option[String] = None,
      numberOfPartitions: Option[Int] = Some(2),
      metadata: Option[Map[String, String]])

  case class PipewrenchConfiguration(configuration: Configuration, tables: Seq[TableDefinition])

  case class TableDefinition(
      sourceName: String,
      destinationName: String,
      checkColumn: Option[String] = None,
      comment: Option[String],
      primaryKeys: Seq[String],
      metadata: Option[Map[String, String]] = None,
      numberOfMappers: Option[Int] = None,
      splitByColumn: Option[String] = None,
      numberOfPartitions: Option[Int] = None,
      columns: Seq[ColumnDefinition])

  case class ColumnDefinition(
      sourceName: String,
      destinationName: String,
      dataType: String,
      comment: Option[String] = None,
      precision: Option[Int] = None,
      scale: Option[Int] = None)

  case class TypeMapping(dataTypes: Map[String, Map[String, String]])

}
