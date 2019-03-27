## 2.0
####Added:
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

####Changed:
- Directory structure for script and config creation.
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

  
####Fixes:
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