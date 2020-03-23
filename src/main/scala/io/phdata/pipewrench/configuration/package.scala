/*
 * Copyright 2018 phData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.phdata.pipewrench

package object configuration {

  case class Configuration(
      name: String,
      environment: String,
      pipeline: String,
      jdbc: Jdbc,
      hadoop: Option[Hadoop] = None,
      snowflake: Option[Snowflake] = None,
      tables: Option[Seq[TableDefinition]] = None)

  case class Jdbc(
      driverClass: Option[String],
      url: String,
      username: String,
      passwordFile: String,
      jceKeyStorePath: Option[String],
      keystoreAlias: Option[String],
      schema: String,
      tableTypes: Seq[String],
      tables: Option[Seq[Table]],
      metadata: Option[Map[String, String]])

  case class Hadoop(
      impalaShellCommand: String,
      stagingDatabase: Database,
      reportingDatabase: Database)

  case class Snowflake(
      snowSqlCommand: String,
      storagePath: String,
      storageIntegration: String,
      warehouse: String,
      taskSchedule: String,
      stagingDatabase: Database,
      reportingDatabase: Database)

  case class Database(name: String, path: Option[String] = None, schema: Option[String] = None)

  case class Table(
      name: String,
      checkColumn: Option[String] = None,
      numberOfMappers: Option[Int] = Some(1),
      splitByColumn: Option[String] = None,
      numberOfPartitions: Option[Int] = Some(2),
      metadata: Option[Map[String, String]])

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
}
