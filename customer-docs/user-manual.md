![Streamliner Logo](../images/streamliner_logo.png)

- [Introduction](#introduction)
  * [Why Streamliner](#why-streamliner)
- [User Guide](#user-guide)
  * [Configuration](#configuration)
    + [Pipeline Configuration](#pipeline-configuration)
    + [Data Source Configuration](#data-source-configuration)
      - [Jdbc Data Source](#jdbc-data-source)
        * [Allowed Schema Changes](#allowed-schema-changes)
      - [AWS Glue Data Catalog](#aws-glue-data-catalog)
        * [User Defined Table](#user-defined-table)
        * [Hadoop User Defined Table](#hadoop-user-defined-table)
        * [Snowflake User Defined Table](#snowflake-user-defined-table)
          + [File Format](#file-format)
    + [Destination Configuration](#destination-configuration)
      - [Hadoop Destination](#hadoop-destination)
        * [Hadoop Database](#hadoop-database)
      - [Snowflake Destination](#snowflake-destination)
        * [Snowflake Database](#snowflake-database)
        * [Table Name Strategy](#table-name-strategy)
          * [Search Replace](#search-replace)
    + [Initial Configuration Example](#initial-configuration-example)
      - [Jdbc Hadoop Configuration](#jdbc-hadoop-configuration)
      - [Glue Snowflake Configuration](#glue-snowflake-configuration)
  * [Templates](#templates)
    + [Make targets](#make-targets)
      - [Command Types](#command-types)
      - [Actual Commands](#actual-commands)
    + [Hadoop Templates](#hadoop-templates)
      - [Incremental With Kudu](#incremental-with-kudu)
      - [Kudu Table DDL](#kudu-table-ddl)
      - [Truncate Reload](#truncate-reload)
    + [Snowflake Templates](#snowflake-templates)
      - [Snowflake AWS DMS Merge](#snowflake-aws-dms-merge)
      - [Snowflake Snowpipe Append](#snowflake-snowpipe-append)
  * [QA Process](#qa-process)
    + [Overview](#qa-overview)
    + [Process](#qa-process)
    + [What it does and doesn't do](#qa-dos-don'ts)
  * [Installing Streamliner](#installing-streamliner)
  * [Executing Streamliner](#executing-streamliner)
    + [Schema Parsing](#schema-parsing)
    + [Script Generation](#script-generation)
  * [Migrating Templates from Streamliner 4.x to 5.x](#migrating-templates-from-streamliner-4x-to-5x)
  * [Connect streamliner to hive with jdbc kerberos authentication](#connect-streamliner-to-hive-with-jdbc-kerberos-authentication)

# Introduction
Streamliner is a Data Pipeline Automation tool that simplifies the process of ingesting data onto a new platform. It is not a data ingestion tool in and of itself; rather, it automates other commonly used tools that ingest and manipulate data on platforms like Snowflake, Amazon Redshift, Cloudera, and Databricks.

Streamliner uses SchemaCrawler to query database table metadata through JDBC. It then generates pipeline code from a series of templates, which can be used to ingest or transform data.

For example, in order to perform Change Data Capture (CDC) -based continuous replication from a source system to Snowflake, Streamliner could be used to generate the SnowSQL needed to create the required Snowpipe tasks, staging tables, and Snowflake Streams, and merge tasks.

![Streamliner pipeline example](../images/streamliner_pipeline.png)

Alternatively, Streamliner might also be used in a very different use case to facilitate data ingestion from relational databases (DBMS) to Hadoop by generating the necessary Sqoop ingest scripts and Impala merge statements.

![Streamliner_hadoop example](../images/streamliner_hadoop_example.png)

## Why Streamliner
- Ingest hundreds or thousands of data sources quickly and easily
- Quickly develop highly complex, templated, and reusable data pipelines into Snowflake, Amazon Redshift, Cloudera, and Databricks.
- Automate the ingestion of business and technical metadata, and the generation of data catalog artifacts, including documentation, ERDs, and integration code.
- Quickly respond to changing requirements like new columns, changing metadata, and new data sources.

# User Guide
Streamliner uses configuration files which applied to templates to create DDL and DML statements which produce complex data ingestion pipelines.  There are two main function performed in Streamliner:
1. `schema` metadata parsing - Collects table metadata information from a relational database or Amazon Web Services Glue Data Catalog.  The output of the `schema` command enhances the original configuration to the be applied to pipeline templates.
2. `scripts` DDL and DML creation - Applies configuration properties to templates to produce DDL and DML scripts.  Templates are a collection of operations producing a data ingestion pipeline for a single table.

## Configuration
The initial Streamliner configuration contains properties about the data source (Jdbc or Glue AWS data catalog) and destinations (Hadoop or Snowflake).  The configurations are created as yaml files and passed into the `schema` and `scripts` subcommands on execution.

Environment variable can be set to the property. Example `url: "${env:URL}"`. Streamliner will read the url from the environment variable URL. 

### Pipeline Configuration
The base level pipeline configuration has 3 properties that define the pipeline.

| Property | Data Type | Required | Comment |
| --- | --- | --- | --- |
| name | String | True | A unique identifier for the pipeline (ex: JD Edwards North America) |
| environment | String | True | Environmental classifier (ex: DEV, TEST, PROD) |
| pipeline | String | True | The name of the pipeline template (ex: snowflake-snowpipe-append or truncate-reload) |
| source | Source | [Data Source Configuration](#data-source-configuration) |
| destination | Destination | [Destination Configuration](#destination-configuration) |

### Data Source Configuration
Streamliner collects metadata from two types of sources:

- [Jdbc](#jdbc-data-source)
- [Glue](#aws-glue-data-catalog)

Only one `source` object can be defined per ingest configuration.

#### Jdbc Data Source
The Jdbc data source configuration defines connection strings and other attributes for relational database management systems.  These configurations will be used by SchemaCrawler to collect metadata on the source system and enhance the configuration with table and column level metadata.

| Property | Data Type | Required | Comment |
| --- | --- | --- | --- |
| type | String | True | Jdbc |
| url | String | True | The jdbc url for the source system (ex: jdbc:oracle:thin:@{host}:{port}:{sid}) |
| username | String | True | JDBC connection username |
| passwordFile | String | True | Only applicable when destination is Hadoop, should be empty string when destination is Snowflake which will use `--database-password`. Location of the password file stored using Hadoop File System. |
| schema | String | True | The schema on the source system to parse metadata from. |
| tableTypes | List[String] | True | Controls which objects get parsed on the source system acceptable values are **table** and **view** |
| metadata | Map[String, String] | False | Global metadata map of key value pairs added as metadata on all tables at creation |
| userDefinedTable | List[UserDefinedTable] | False | [User defined table](#user-defined-table) attributes (override tables metadata to be parsed) |
| tables | List[String] | False | Controls which tables to be crawled from source system. |
| ignoreTables | List[String] | False | Controls which tables  to be ignored from schema crawling. |
| validSchemaChanges | List[String] | False | Controls the [Allowed Schema Changes](#allowed-schema-changes) in schema evolution. |

#### Allowed Schema Changes
* TABLE_ADD
* COLUMN_ADD
* UPDATE_COLUMN_COMMENT
* UPDATE_COLUMN_NULLABILITY
* EXTEND_COLUMN_LENGTH

#### AWS Glue Data Catalog
The AWS Glue data catalog configuration defines which Glue Data Catalog database to collect metadata from.

| Property | Data Type | Required | Comment |
| --- | --- | --- | --- |
| type | String | True | Glue |
| region | String | True | AWS region (ex. us-east-1)
| database | String | True | AWS Glue data catalog database to parse table list from |
| userDefinedTable | List[UserDefinedTable] | False | [User defined table](#user-defined-table) attributes |

##### User Defined Table
The user defined table configuration object allows the user to enhance the metadata about a specific table with custom properties.

##### Hadoop User Defined Table
Additional metadata properties for Hadoop specific templates.  Allows the user to supply metadata properties that are not discoverable on the source RDMS or Glue systems.

| Property | Data Type | Required | Comment |
| --- | --- | --- | --- |
| type | String | True | Hadoop |
| name | String | True | Source system table name |
| primaryKeys | Seq[String] | False | A user provided list of primary key columns |
| checkColumn | String | False | The incremental column that is 'checked when using Sqoop incremental import |
| numberOfMappers | String | False | The number of mappers to be used by Sqoop during import |
| numberOfPartitions | String | False | The number of partitions to be created when using pipelines that create Kudu tables |
| metadata | Map[String, String] | False | Table metadata map of key value pairs added as metadata on all tables at creation |

##### Snowflake User Defined Table
Additional metadata properties for Snowflake specific templates.  Allows the user to supply metadata properties that are not discoverable on the source DMS or Glue systems.

| Property | Data Type | Required | Comment |
| --- | --- | --- | --- |
| type | String | True | Snowflake |
| name | String | True | Source system table name |
| primaryKeys | Seq[String] | False | A user provided list of primary key columns |
| fileFormat | [FileFormat](#file-format) | False | User provided file format definition |

###### File Format
Snowflake file format definition

| Property | Data Type | Required | Comment |
| --- | --- | --- | --- |
| location | String | True | Cloud storage location |
| fileType | String | True | File type |
| delimiter | String | False | Field delimiter used on separated text file formats |
| nullIf | Seq[String] | False | A list of values that should be converted to NULL if found on ingest |

### Destination Configuration
Properties defining the data platform being ingested into.  Streamline supports the following destination types:

- [Hadoop](#hadoop-destination)
- [Snowflake](#snowflake-destination)

Only one destination configuration can be defined per ingest configuration.

#### Hadoop Destination
Configuration properties defining the Hadoop destination.

| Property | Data Type | Required | Comment |
| --- | --- | --- | --- |
| type | String | True | Hadoop |
| impalaShellCommand | String | True | Impala shell command used to execute sql statements from produced DML and DDL scripts |
| stagingDatabase | [HadoopDatabase](#hadoop-database) | True | Staging database in the Hadoop environment |
| reportingDatabase | [HadoopDatabase](#hadoop-database) | True | Reporting or Modeled database in the Hadoop environment |

##### Hadoop Database
Configuration properties defining the Hadoop database.

| Property | Data Type | Required | Comment |
| --- | --- | --- | --- |
| name | String | True | The name of the Hadoop database |
| path | String | True | The HDFS path to the Hadoop database |
 
#### Snowflake Destination
Configuration properties defining the Snowflake destination.

| Property | Data Type | Required | Comment |
| --- | --- | --- | --- |
| type | String | True | Snowflake |
| snowSqlCommand | String | True | SnowSQL cli command used to execute sql statements from produced DML and DDL scripts |
| storagePath | String | True | Cloud storage location to find the schema level table objects from (used when creating external stage) |
| storageIntegration | String | True | Name of the Snowflake storage integration used when created an external stage |
| snsTopic | String | False | Name of the AWS SNS Topic to be used when configuring Snowpipe. |
| warehouse | String | True | Name of the Snowflake warehouse to be used when executing tasks and copy into commands |
| taskSchedule | String | False | Snowflake task schedule |
| stagingDatabase | [SnowflakeDatabase](#snowflake-database) | True | Staging database where data will staged into in Snowflake |
| reportingDatabase | [SnowflakeDatabase](#snowflake-database) | True | Reporting database where data will be merged into or replicated in Snowflake |
| tableNameStrategy | [TableNameStrategy](#table-name-strategy) | False | Option to modify snowflake table name. |

##### Snowflake Database
Configuration properties defining the Snowflake database.

| Property | Data Type | Required | Comment |
| --- | --- | --- | --- |
| name | String | True | Snowflake database name |
| schema | String | True | Snowflake schema name |

##### Table Name Strategy
Configuration properties defining the `tableNameStrategy`. 

| Property | Data Type | Required | Comment |
| --- | --- | --- | --- |
| asIs | Boolean | False | No change in the snowflake tables name. |
| addPostfix | String | False | This will add a postfix to the snowflake tables name. |
| addPrefix | String | False | This will add a prefix to the snowflake tables name. |
| searchReplace | [SearchReplace](#search-replace) | False | Option to search and replace string in the snowflake table name. |

##### Search Replace
Configuration properties defining the `searchReplace`.

| Property | Data Type | Required | Comment |
| --- | --- | --- | --- |
| search | String | True | String to be searched in the table name. Regex can also be used. |
| replace | String | True | String to replace the searched string. |
| occurrences | Integer[] | False | The searched string will be replaced only at provided occurrence values. |


### Initial Configuration Example

#### Jdbc Hadoop Configuration
```yaml
name: JDEdwardsNorthAmerica
environment: prod
pipeline: incremental-with-kudu
source:
  type: Jdbc
  url: "jdbc:oracle:thin:@{host}:{port}:{sid}"
  username: INGEST_USER
  passwordFile: "/home/ingest_user/jdedwards_na/.password"
  schema: "JDE"
  tableTypes:
    - table
destination:
  type: Hadoop
  impalaShellCommand: impala-shell -f
  stagingDatabase:
    name: JDE_NA_RAW
    path: "hdfs:/data/raw/jde_na/"
  reportingDatabase:
    name: JDE_NA
    path: "hdfs:/data/modeled/jde_na"
```

#### Glue Snowflake Configuration
```yaml
name: InternalSalesForecastingDatabase
environment: prod
pipeline: snowflake-snowpipe-append
source:
  type: Glue
  region: us-east-1
  database: sales_forecast
destination:
  type: Snowflake
  snowSqlCommand: snowsql -c phdata
  storagePath: "s3://{bucket}/{path}"
  storageIntegration: "STORAGE_INTEGRATION_READ"
  warehouse: "DEFAULT_WH"
  taskSchedule: "5 minutes"
  stagingDatabase:
    name: SALES_FORECAST
    schema: STAGE
  reportingDatabase:
    name: SALES_FORECAST
    schema: RAW
```

## Templates
Templates define a data ingestion pipeline for a single table within a schema or database.  Templates generate DDL and DML statements and are written using [Scala Server Pages](https://scalate.github.io/scalate/documentation/ssp-reference.html).  Streamliner has templates for automating data pipelines in Snowflake and Hadoop data platforms.

### Make targets

Targets are a function of a template. Therefore, it's possible for a template to define custom targets. The table below
documents standards we expect all templates to follow.

#### Command Types

| Syntax      | Description |
| ----------- | ----------- |
|`first-run`  | `first-run` will create tables and provision objects necessary to complete the data pipeline. For example creating a Snowflake stage.|
|`run`        | `run`s are executed on an on-going basis and should be scheduled. These are typically only used for Hadoop based batch data pipelines. |
|`evolve-schema` |`evolve-schema` keeps your target destination tables in sync with a source system and should be scheduled. This command stores state in source control. If you have already executed `first-run` then you will need to generate the appropriate state.|
|`drop`        |`drop` will drop or destroy all objects related to the data pipeline.  |

#### Actual Commands

| Syntax      | Description |
| ----------- | ----------- |
|`first-run-all` | Run `first-run` on all configured tables.|
|`first-run-<TABLE_NAME>`  | Run `first-run` only on `<TABLE_NAME>`|
|`run-all` | Run `run` on all configured tables.|
|`run-<TABLE_NAME>`  | Run `run` only on `<TABLE_NAME>`|
|`evolve-schema-all` | Run `evolve-schema` on all configured tables.|
|`evolve-schema-<TABLE_NAME>`  | Run `evolve-schema` only on `<TABLE_NAME>`|
|`drop-all` | Run `drop` on all configured tables.|
|`drop-<TABLE_NAME>`  | Run `drop` only on `<TABLE_NAME>`|

### Hadoop Templates
#### Incremental With Kudu
See `templates/hadoop/incremental-with-kudu`
Creates an incremental ingestion pipeline using Sqoop and Apache Kudu.

`first-run` Steps:
1. Create a sqoop job definition for incremal batch loading into Hadoop avro table
2. Execute sqoop job full load
3. Copy avsc.json (avro schema) to archive table location
4. Copy avsc.json (avro schema) to staging table location
5. Create archive table.  The archive table appends all incremental sqoop runs into a single table for auditing.
6. Create report Kudu table.
7. Create staging table.  The staging table which sources the UPSERT INTO kudu table operation.
8. Compute stats on the staging or increment table.
9. UPSERT INTO kudu table FROM staging table
10. Compute states on the kudu table

`run` Steps:
1. Copy last increment into archive table
2. Invalidate metadata on archive table to refresh newly loaded increment
3. Compute stats on the archive table
4. Invalidate metadata on Staging table to remove reference to previously loaded files
5. Execute sqoop job incremental
6. Copy avsc.json (avro schema) to archive table location
7. Copy avsc.json (avro schema) to staging table location
8. Compute stats on the staging or increment table.
9. UPSERT INTO kudu table FROM staging table
10. Compute states on the kudu table

`drop` Steps:
1. Delete the sqoop job definition
2. Drop the archive table
3. Drop the report table
4. Drop the staging table
5. Delete the archive HDFS data
6. Delete the staging HDFS data
7. Delete the generated avro schema definitions

#### Kudu Table DDL
See `templates/hadoop/kudu-table-ddl`
Creates kudu tables.

`first-run` Steps:
1. Create Kudu table
2. Compute stats on Kudu table

`run` Steps:
1. Compute stats on Kudu table

`drop` Steps:
1. Drop Kudu table

#### Truncate Reload
See `templates/hadoop/truncate-reload`
Build a full truncate and reload data pipeline using Sqoop and Impala tables.  Snapshots of the last 5 data loads are kept in an archive table to reduce the need to re-run the ingest from source if data needs to be reloaded in the Impala table.

`first-run` Steps:
1. Execute Sqoop full load job
2. Copy avsc.json (avro schema) to hdfs table directory
3. Create partitioned or archive table
4. Create reporting table
5. Create staging table
6. Invalidate Impala meta data on staging table
7. Switch the reporting table location to latest loaded partition
8. Compute stats of partitioned table
9. Compute stats on reporting table
10.  Validate row count between source system and Impala table

`run` Steps:
1. Execute Sqoop job
2. Copy avsc.json (avro schema) to hdfs table directory
3. Invalidate Impala metadata on staging table
4. Compute stats on staging table
5. Compute stats on reporting table
6. Validate row count between source system and Impala table

`drop` Steps:
1. Drop partitioned table
2. Drop report table
3. Drop staging table
4. Delete partitioned HDFS data
5. Delete reporting HDFS data
6. Delete staging HDFS data
7. Delete generated HDFS avro schema

### Snowflake Templates
#### Snowflake AWS DMS Merge
See `templates/snowflake/snowflake-aws-dms-merge`
The AWS DMS Merge template is Snowflake data pipeline incrementally ingesting CDC records from Amazon Database Migration (DMS) service into Snowflake.

Snowflake's continous ingest tool Snowpipe automatically copies data into the staging table once the files are written to the external stage.  A scheduled Snowflake Task then merges the records into the reporting table.

*NOTE:* After `run` is executed once there is no need to schedule this template externally.

`first-run` Steps:
1. Create staging schema
2. Create staging table
3. Create report schema
4. Create report table
5. Create external stage for table referencing Snowflake Storage integration
6. Create Snowflake Stream on staging table for incremental change tracking
7. COPY INTO staging table bulk historical records
8. Create Snowflake Task to MERGE INTO reporting from Stream
9. Schedule task execution

`run` Steps:
1. Copy any incremental CDC events into staging table
2. Create Snowpipe for ongoing ingestion

`drop` Steps:
1. Suspend task execution
2. Drop Snowflake task
3. Drop Snowpipe
4. Drop Stream
5. Drop report table
6. Drop staging table

*NOTE:* `drop` does not remove the `staging` and `report` schemas nor does it the `stage` which can be cleaned by `make drop-stage`.

#### Snowflake Snowpipe Append
See `templates/snowflake/snowflake-snowpipe-append`
The Snowpipe Append template is A Snowflake data pipeline to append newly arriving data from a Storage Integration into Snowflake.  Snowpipe is used to continously load data from cloud storage accounts into Snowflake.

*NOTE:* Once `first-run` is executed this pipeline does not need to be scheduled by externally. 

`first-run` Steps:
1. Create schema
2. Create stage
3. Create table
4. Copy into table from external stage
5. Create Snowpipe

`drop` Steps:
1. Drop table
2. Drop Snowpipe

*NOTE:* `drop` does not remove the schema nor does it the `stage` which can be cleaned by `make drop-stage`.

## QA Process 

### Overview
When creating data pipelines, measuring quality is always very important. Streamliner supports templates for collecting metrics
on columns of tables and compare them over time. This method is based off of [control charts](https://en.wikipedia.org/wiki/Control_chart)
to measure the variablility of the amount of data over time.

### Process
To take a control chart process and convert it to pipelines the process needs to complete the following steps:

1.	Capture the occurrences of data values across all columns in a table.
2.	Not capture data that is small or a small percentage of the values in a column.
3.	Store each “run” of data in a metrics vault.
4.	Compare the current “run” to the average and standard deviation of the previous runs for each value in each column of the table.

Starting out with collecting and storing the data, in every database using this process we would want to create a “Metrics” table to store all of the data. That table would have the time and date of the run, the column name being looked at, the value in that column, and the count of records with that value. To collect the data in snowflake, a stored procedure can find all the columns on the table, calculate the counts for each value, and insert the data into the “Metrics” table.

To easily collect the data, we can add a stream to the table we are monitoring and have a task that runs the stored procedure to collect the data. Once the data is collected and stored, the stored procedure can check if the values are within the UCL and LCL or 3 standard deviations away from the historical mean. If there are any values outside the range, the stored procedure can call an external api alerting users that some of the date requires investigation. Below is a diagram showing the dataflow.

![QA_process](../images/streamliner-qa-process.png)

#### Dos and Don'ts
This process should be used when monitoring on ongoing pipeline. It will detect if a statistical
anomaly in the values of the data. It will not tell you if something is wrong or confirm data fits a 
predefined structure. The metrics repo also needs data to inform if data fits the
historical pattern, so it will take time before the results from the QA Proces become dependable.

Constraints on tables, proper monitoring of logs, and unit tests are still recommended.

The metrics repo can also be connected to from a visualization tool to allow
users dig into the values and how they change over time.

## Installing Streamliner
You can find the latest version of streamliner in [phData's Artifactory](https://cloudsmith.io/~phdata/repos/streamliner/packages/).

The artifact is a zip file contains an executable in `bin/streamliner`, templates in `templates`, and example config in `conf`.

## Executing Streamliner

### Schema Parsing
Before executing the schema parsing functionality of Streamliner the developer must first create the [initial ingest configuration](#initial-configuration-example)

CLI Arguments:

| name | type | Required | Comment |
| --- | --- | --- | --- |
| config | String | True | Location of the initial ingest configuration file |
| state-directory | String | True | Current run table config directory where Streamliner configuration per table will be written. |
| previous-state-directory | String | True | Previous run table config directory where Streamliner configuration per table is written to. |
| database-password | String | False | Relational database password used when parsing Jdbc source types. Not used when importing to Hadoop which uses the `passwordFile` Yaml key. |
| create-docs | Boolean | False | Control flag to indicate whether an ERD and HTML file should be created when parsing Jdbc source types |
| log-level | String | False | Parameter to change the application log level. Log level set in `log4j.properties` file present in `conf` folder is the default value. |

CMD: `<install directory>/bin/streamliner schema --config conf/private-ingest-configuration.yml --state-directory <state-directory-path> --database-password <pass> --log-level INFO`

### Script Generation
Schema parsing must be executed before executing script generation as the table definitions are needed to create the data pipelines.

CLI Arguments:

| name | type | Required | Comment |
| --- | --- | --- | --- |
| config | String | True | Location of the initial ingest configuration file |
| state-directory | String | True | Current run table config directory where Streamliner configuration per table is written to. |
| previous-state-directory | String | True | Previous run table config directory where Streamliner configuration per table was written to. |
| output-path | String | True | Location where Streamliner output should be written to. |
| type-mapping | String | True | Location of the type-mapping.yml file |
| template-directory | String | True | Location of the templates |
| log-level | String | False | Parameter to change the application log level. Log level set in `log4j.properties` file present in `conf` folder is the default value. |

CMD : `<install directory>/bin/streamliner scripts --config conf/private-ingest-configuration.yml --state-directory <state-directory-path> --previous-state-directory <previous-state-directory-path> --type-mapping <install-directory>/conf/type-mapping.yml  --template-directory <install directory>/templates/<snowflake | hadoop> --output-path <script-output-path> --log-level INFO`

## Migrating Templates from Streamliner 4.x to 5.x
1. Replace the imports and variables to use new Java POJO classes.
   
   Example : 
   
   Streamliner 4.x: 
   
   `#import(io.phdata.streamliner.configuration.Snowflake)`
   
   `<%@ val configuration: io.phdata.streamliner.configuration.Configuration %>`
   
   Streamliner 5.x:

   `#import(io.phdata.streamliner.schemadefiner.model.Snowflake)`
   
   `<%@ val configuration: io.phdata.streamliner.schemadefiner.model.Configuration %>`
2. Replace the old scala POJO class methods with equivalent new POJO class method definition. Pass the arguments correctly if needed.
   
   Example:
   
   Streamliner 4.x:
   
   ```
    CREATE TASK IF NOT EXISTS ${destination.stagingDatabase.name}.${destination.stagingDatabase.schema}.${table.destinationName}_task
    WAREHOUSE = ${destination.warehouse}
    SCHEDULE = '${destination.taskSchedule.getOrElse("5 minutes")}'
    WHEN SYSTEM$STREAM_HAS_DATA('${table.destinationName}_stg_stream')
    AS
    MERGE INTO ${destination.reportingDatabase.name}.${destination.reportingDatabase.schema}.${table.destinationName} t
        USING ( SELECT ${table.columnList(Some("si"))}, si.dms_operation, i.max_dms_ts
                FROM ${destination.stagingDatabase.name}.${destination.stagingDatabase.schema}.${table.destinationName}_stg_stream si
                INNER JOIN ( SELECT ${table.pkList}, MAX(dms_ts) max_dms_ts
                             FROM ${destination.stagingDatabase.name}.${destination.stagingDatabase.schema}.${table.destinationName}_stg_stream
                             GROUP BY ${table.pkList} ) i
                ON ${table.pkConstraint("i", "si")} AND i.max_dms_ts = si.dms_ts ) s
    ON ${table.pkConstraint("t", "s")}
        WHEN MATCHED AND s.dms_operation = 'U' THEN UPDATE SET ${table.columnConstraint(bAlias = "s", joinCondition = ", ")}, dms_operation = s.dms_operation, dms_ts = s.max_dms_ts
        WHEN MATCHED AND s.dms_operation = 'D' THEN DELETE
        WHEN NOT MATCHED AND s.dms_operation != 'D' OR s.dms_operation IS NULL THEN INSERT (${table.columnList()}, dms_operation, dms_ts) VALUES (${table.columnList(Some("s"))}, s.dms_operation, s.max_dms_ts);
   ```
   
   Streamliner 5.x:

   ```
    CREATE TASK IF NOT EXISTS ${destination.stagingDatabase.name}.${destination.stagingDatabase.schema}.${table.destinationName}_task
    WAREHOUSE = ${destination.warehouse}
    SCHEDULE = '${destination.taskSchedule}'
    WHEN SYSTEM$STREAM_HAS_DATA('${table.destinationName}_stg_stream')
    AS
    MERGE INTO ${destination.reportingDatabase.name}.${destination.reportingDatabase.schema}.${table.destinationName} t
        USING ( SELECT ${table.columnList("si")}, si.dms_operation, i.max_dms_ts
                FROM ${destination.stagingDatabase.name}.${destination.stagingDatabase.schema}.${table.destinationName}_stg_stream si
                INNER JOIN ( SELECT ${table.pkList}, MAX(dms_ts) max_dms_ts
                             FROM ${destination.stagingDatabase.name}.${destination.stagingDatabase.schema}.${table.destinationName}_stg_stream
                             GROUP BY ${table.pkList} ) i
                ON ${table.pkConstraint("i", "si", null)} AND i.max_dms_ts = si.dms_ts ) s
    ON ${table.pkConstraint("t", "s", null)}
        WHEN MATCHED AND s.dms_operation = 'U' THEN UPDATE SET ${table.columnConstraint(null, "s", ", ")}, dms_operation = s.dms_operation, dms_ts = s.max_dms_ts
        WHEN MATCHED AND s.dms_operation = 'D' THEN DELETE
        WHEN NOT MATCHED AND s.dms_operation != 'D' OR s.dms_operation IS NULL THEN INSERT (${table.columnList(null)}, dms_operation, dms_ts) VALUES (${table.columnList("s")}, s.dms_operation, s.max_dms_ts);
   ```
3. Since Streamliner 5.x is written in JAVA, we might have to convert few Java code into Scala to support in SSP template. For example, we converted Java List to Scala Seq to use few scala methods in SSP template.

   Example:

   Streamliner 4.x:

   ```
    CREATE TABLE IF NOT EXISTS ${destination.reportingDatabase.name}.${destination.reportingDatabase.schema}.${table.destinationName} (
    #for (column <- table.columns)
    ${unescape(column.destinationName)} ${column.mapDataTypeSnowflake(typeMapping)} COMMENT '${column.comment.getOrElse("")}',
    #end
    dms_operation CHAR COMMENT 'AWS DMS Operation type',
    dms_ts TIMESTAMP_LTZ COMMENT 'AWS DMS timestamp'
    )
    COMMENT = '${table.comment.getOrElse("")}';
   ```

   Streamliner 5.x:

   ```
    CREATE TABLE IF NOT EXISTS ${destination.reportingDatabase.name}.${destination.reportingDatabase.schema}.${table.destinationName} (
    #for (column <- util.convertListToSeq(table.columns))
    ${unescape(column.destinationName)} ${column.mapDataTypeSnowflake(typeMapping)} COMMENT '${column.comment}',
    #end
    dms_operation CHAR COMMENT 'AWS DMS Operation type',
    dms_ts TIMESTAMP_LTZ COMMENT 'AWS DMS timestamp'
    )
    COMMENT = '${table.comment}';
   ```

## Connect streamliner to hive with jdbc kerberos authentication

1. Connect to hive edge node.
2. Create ticket cache using `kinit` command. Or to create ticket cache using keytab file, please follow these steps:
   1. Execute `ktutil` command.
   2. Execute 
    ```
       add_entry -password -p yourusername@YOURDOMAIN -k 1 -e aes256-cts
    ```
   3. Provide password for yourusername@YOURDOMAIN
   4. Execute
    ```
        wkt yourusername.keytab
    ```
   5. Execute `exit` command.
   6. Till step 5, a keytab should be created.
   7. To create the ticket cache using keytab file, execute command
    ```
        kinit yourusername@YOURDOMAIN -k -t yourusername.keytab
    ```
3. Validate hive jdbc connection using beeline. Please make sure ticket cache exists and it's not expired using `klist` command.
   ```
       beeline -u "jdbc:hive2://<host_name>:<port>/<db>;principal=<hive_princ_name>"
   ```
4. Copy streamliner to hive edge node.
5. Provide correct hive jdbc url to streamliner config. The hive jdbc url should look like below
    ```
        jdbc:hive2://<host_name>:<port>/<db>;AuthMech=1;KrbHostFQDN=<host_name>;KrbServiceName=<service_name>;KrbRealm=<Realm>;SSL=1;SSLTrustStore=<ssl_trust_store>
    ```
6. Execute streamliner schema command from streamliner folder. Please make sure ticket cache exits before executing schema command.   