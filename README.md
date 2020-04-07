# Streamliner

Data Pipeline Automation Tool

## How it works

streamliner uses [SchemaCrawler](https://www.schemacrawler.com/) to query database table metadata throught JDBC, then generates pipeline code
from a series of templates that when executed can be used to ingest or transform data.

## Installation

You can find the latest version of streamliner-scala here: https://repository.phdata.io/artifactory/list/binary/phdata/streamliner/

The artifact is a zip file containt an executable in `bin/streamliner` and templates in `bin/templates`

## Config File

streamliner works off the parameters in the ingest-configuration file.


```
name: ""        # Unique name for data ingestion
environment: "" # Environment identifier
pipeline: ""    # Pipeline name must match a directory in the the provided template folder (ie incremental-with-kudu, kudu-table-ddl, truncate-reload)
jdbc:
  url: ""       # JDBC Connection url
  username: ""  # JDBC Username
  passwordFile: "" # HDFS location of password file
  schema: ""    # JDBC Schema / database name
  tableTypes:   # Controls the types of tables included in the configuration output
    - table
    - view
 OPTIONAL: table whitelisting
  tables:
    - name: table1
    - name: table2
hadoop:
  impalaShellCommand: "" # Impala shell command copied from CM
  stagingDatabase:
    name: "" # Staging database
    path: "" # Staging database hdfs path
  reportingDatabase:
    name: "" # Reporting database name (these can be the same as staging database if template allows)
    path: "" # Reporting database path (these can be the same as staging database if template allows)
```

**Note**: if the hadoop credential provider is used for password storage, replace:

```
  passwordFile: "" # HDFS location of password file
```

with

```
  jceKeyStorePath: "" # HDFS location of password file
  keystoreAlias: "" # Alias for the keystore
```

## Type Mapping

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
streamliner schema --config <ingest-configuration.yml> --database-password <database password>
```

With Generating Docs
```bash
streamliner schema --config <ingest-configuration.yml> --database-password <database password> --create-docs
```

### Scripts
Generating scripts:

```bash
streamliner scripts --config <ingest-configuration.yml> --template-directory <template-directory> --type-mapping <type-mapping.yml>
```

## Running Integration Tests
To run a full pipeline in an isolated docker environment:
- Install [docker](https://docs.docker.com/install/) and [docker-compose](https://docs.docker.com/compose/install/) on your local machine.
- Add or modify the template to test in `integration-tests/templates` (or run against current templates).
- Run `make itest`.

Note: the name of the template file should match the pipeline you are trying to test. 
Ex. `integration-tests/templates/kudu-table-ddl.yml`
```
pipeline: kudu-table-ddl
```

## Publishing to Artifactory
### To publish streamliner .zip to Artifactory
- Change the current version to the new version in the `version` file.
- Using the template `build-support/artifactory.env.template`, create `build-support/artifactory.env`.
- In the `build-support/artifactory.env` file add the necessary values for the `ARTIFACTORY_USER` and `ARTIFACTORY_TOKEN` variables.
  - Set `ARTIFACTORY_USER` to the user name you use to log into Artifactory.
  - To get your `ARTIFACTORY_TOKEN`:
    - Log into [phData's Artifactory](https://repository.phdata.io/artifactory).
    - Click on your username on the top right corner.
    - Enter your password and press *Unlock*.
    - Copy the value of *Encrypted Password* and paste it as the value for `ARTIFACTORY_TOKEN`.
- Run `make publish`.

The .zip will show up in [phData's Artifactory binaries](https://repository.phdata.io/artifactory/list/binary/phdata/streamliner/)

### To publish streamliner .jar to Artifactory
- Create a credentials file in your home directory under `[home]/.sbt/.credentials` containing:
```
realm=Artifactory Realm
host=repository.phdata.io
user=[Artifactory username]
password=[Artifactory password]
```
- Change the current version to the new version in the `version` file.
- Run `sbt clean package publish` to publish the .jar.

The release folder will show up in [phData's Artifactory libs](https://repository.phdata.io/artifactory/list/libs-release-local/io/phdata/streamliner/streamliner_2.11/).


