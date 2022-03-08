## 5.6.0
- Added support to connect to Redshift via JDBC.
- [Download](https://repo.phdata.io/public/streamliner/maven/io/phdata/streamliner/streamliner/5.6.0/streamliner-5.6.0.zip)

## 5.5.0
- Introduced a parameter `genericProperties` in ingest-config file to pass custom parameters to the template.
- Updated ingest-config file source schema parameter to accept schema_name or dbname.schema_name.
- [Download](https://repo.phdata.io/public/streamliner/maven/io/phdata/streamliner/streamliner/5.5.0/streamliner-5.5.0.zip)

## 5.4 
- Implemented table naming strategy feature for snowflake tables (ex: adding prefix/postfix, search and replace string)
- [Download](https://repo.phdata.io/public/streamliner/maven/io/phdata/streamliner/streamliner/5.4/streamliner-5.4.zip)

## 5.3
- Migrated streamliner build from SBT to Gradle
- Option to choose allowed schema changes
- Generate `delta-change-summary.txt` file if there are schema changes
- Option to ignore tables while schema crawling
- [Download](https://repo.phdata.io/public/streamliner/maven/io/phdata/streamliner/streamliner/5.3/streamliner-5.3.zip)

## 5.1
- Support extending string-like columns, column comments, and nullability changes
- Documentation updates
- [Download](https://repo.phdata.io/public/streamliner/maven/io/phdata/streamliner/streamliner/5.1/streamliner-5.1.zip)

## 5.0
*Please note that migration of templates is required from Strealiner 4.x to 5.0*
- Support Schema Evolution
- Rename `clean` to `drop`
- Map date to datetime by default and include additional types for oracle
- Support `NOT NULL`
- Ensure primary keys are "quote if needed" like columns
- [Download](https://repo.phdata.io/public/streamliner/maven/io/phdata/streamliner/streamliner/5.0/streamliner-5.0.zip)

## 4.3
- Truncate max length to 16,777,216 when required
- [Download](https://repo.phdata.io/public/streamliner/maven/io/phdata/streamliner/streamliner/4.3/streamliner-4.3.zip)

## 4.2
- Support tables without a primary key, but have a unique index.
- [Download](https://repo.phdata.io/public/streamliner/maven/io/phdata/streamliner/streamliner/4.2/streamliner-4.2.zip)

## 4.1
- Improved type mapping based on user feedback.
- [Download](https://repo.phdata.io/public/streamliner/maven/io/phdata/streamliner/streamliner/4.1/streamliner-4.1.zip)

## 4.0
- Snowflake column names are now quoted if they contain special characters as opposed to “cleansing” them like we do with Hadoop. This is to support easy re-pointing of BI tools, queries, etc.
- Snowflake data types on the other end of the translation are now enhanced. Specifically NUMBER on oracle without precision scale are now NUMBER(38,8) as opposed to string. Please note any custom templates will need upgrading.
- [Download](https://repo.phdata.io/public/streamliner/maven/io/phdata/streamliner/streamliner/4.0/streamliner-4.0.zip)

## 3.4
- Add QA Framework
- Upgrade to SchemaCrawler 16.12.3 which adds Hive Support.
- Numerous documentation updates
- [Download](https://repo.phdata.io/public/streamliner/maven/io/phdata/streamliner/streamliner/3.4/streamliner-3.4.zip)

## 3.3
- Skipped

## 3.2
- Added Quickstart to documentation
- [Download](https://repo.phdata.io/public/streamliner/maven/io/phdata/streamliner/streamliner/3.2/streamliner-3.2.zip)

## 3.1
- Skipped

## 3.0
- Renamed to Streamliner
- [Download](https://repo.phdata.io/public/streamliner/maven/io/phdata/streamliner/streamliner/3.0.0/streamliner-3.0.0.zip)

## 2.3
*Fixed*
- Updated checkNumberOfMappers function to check numberOfMappers in configuration vs numberOfPartitions
- Fixed metadata enhancement to include global metadata

## 2.2
*Added*
- Added sqlserver in source column mapping. 69bc1e4569634921daf8ceab40b44d254413d189
- Ability to use a JCECKS keystore for Sqoop authentication. eb944d32693c42c512ff1fb1399ef6dcb18a9089
- Upgrade the schemacrawler version to 16.2.5. 276a1adb486ef558a91c6d41bc1b8f86a012fcce

*Changed*
- Fixed a bug where a wrong database name was used in a table count template.  69bc1e4569634921daf8ceab40b44d254413d189

## 2.1
*Added*
- Make configuartion & typeMapping variables available to .schema.ssp templates. 
- Adjust schema crawler logging level from the cli using `--crawler-log-level`. 
- Add a default logback.xml file. e8d4957e8952b378e35800e2fcfe8abf07a009ad
- Strip the 'identity' suffix from some columns. d81b10a076ebe4a60bab8fd87471b9f011cc3568
- Add validate rowcount functionality in the truncate-reload templates. 
- Preserve the order of tables and columns when writing config files. 

## 2.0
*Added*
- Full integration testing of pipelines `make -C integration-tests/ itest`.
- Enum type in type-mapping.
- Mysql .jar in `lib/` directory.
- Makefile for common tasks.
- Apache 2 license.
- Test template rendering.
  - Add basic sanity tests for template rendering.
  - Adds a check for `splitByColumn` to avoid a `None.get` error.
- Log currently rendering file.
  - If it fails we can look up the log stack and see exactly which template failed.

*Changed*
- Directory structures for script and config creation.
- Shortened `--configuration` to `--config` for the lazy. 
  - Use the subcommand `schema` to refer to all things external db schema related in place of `configuration`.
- Remove log files and ignore logs.
- Switch to circe-yaml parsing.
  - Circe provides 'AutoDerivation' which removes the need for defining `YamlFormatN` implicit classes.
  - Circe and MoultingYaml are both wrappers around SnakeYaml. Circe seems to be better supported.
    - This is a first step in providing generic configurations when a user doesn't necessarily want to ingest data from a relational source into Hadoop (for example StreamSets, Nifi pipeline generation).
- Removed all `use` statements that have fully qualified tables.
  - Modified database in compute-stats-staging.sql.ssp from reporting to staging.
  - Changed table->tables in initial ingest-configuration and README.md.
- Use `$check_b` value instead of `exit 1` in run-with-logging
- Refactored the version to read from file.

*Fixes*
- OrderColumns function to create-report-table in the incremental-with-kudu template to fix ordering of columns in Kudu table ddl.
- Removed newline created in incremental-with-kudu/create-sqoop-job.sh.ssp.
- False targets in make file for incremental-with-kudu.
- Modified archive-staging-data template to use correct case class path and changed path to reference DIRECTORY variable.
- Added incremental append to incremental-with-kudu sqoop template.
- Changed ordering of kudu ddl to list primary key columns first.
- Fix regression with set executable. 
  - Check if the template is executable, if it is set the target as executable. 
  - Adds unit tests. 
  - Changes name to `setExecutable` to show modification of the file can happen.
- Bash options:
  - Remove `-x`, users can add it if it's needed. If it's always on it's too verbose.
  - The -x was added last, so it interfered with `-o pipefail`, which need to be in order. The effect was that the `pipefail` option wasn't being applied.
  - Fixed the run-with-logging script. It does its own error handling and should not have the `-e` option. It also checks its own pipefailure, so removed `-o pipefail`.
- Oracle timestamps with leading schema identifier.
- Fixed staging table type-mapping.
