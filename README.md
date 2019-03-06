# Pipewrench

Hadoop Data Pipeline Automation Tool

## How it works

Pipewrench uses [SchemaCrawler](https://www.schemacrawler.com/) to query database table metadata throught JDBC, then generates pipeline code
from a series of templates that when executed can be used to ingest or transform data.

## Config File

Pipewrench works off the parameters in the ingest-configuration file.


```
name: "" # Unique name for data ingestion
environment: "" # Environment identifier
pipeline: "" # Pipeline name must match a directory in the the provided template folder (ie incremental-with-kudu, kudu-table-ddl, truncate-reload)
jdbc:
  url: "" # JDBC Connection url
  username: "" # JDBC Username
  passwordFile: "" # HDFS location of password file
  schema: "" # JDBC Schema / database name
  tableTypes: # Controls the types of tables included in the configuration output
    - table
    - view
# OPTIONAL: table whitelisting
#  tables:
#    - name: table1
#    - name: table2
hadoop:
  impalaShellCommand: "" # Impala shell command copied from CM
  stagingDatabase:
    name: "" # Staging database
    path: "" # Staging database hdfs path
  reportingDatabase:
    name: "" # Reporting database name (these can be the same as staging database if template allows)
    path: "" # Reporting database path (these can be the same as staging database if template allows)
```

### Type Mapping

The type mapping file is used to convert data types from source to target system.

```
bigint:
  kudu: bigint
  impala: bigint
  parquet: bigint
  avro: bigint
tinyint:
  kudu: int
  impala: int
  parquet: int
  avro: int
decimal:
  kudu: string
  impala: decimal
  parquet: decimal
  avro: string
```

## Usage

Generating ingest configuration from a database (`ingest-configuration.yml`):

### Configuration
Without Generating Docs
```bash
pipewrench schema --config <ingest-configuration.yml> --database-password <database password>
```

With Generating Docs
```bash
pipewrench schema --config <ingest-configuration.yml> --database-password <database password> --create-docs
```

### Scripts
Generating scripts:

```bash
pipewrench scripts --config <ingest-configuration.yml> --template-directory <template-directory> --type-mapping <type-mapping.yml>
```
