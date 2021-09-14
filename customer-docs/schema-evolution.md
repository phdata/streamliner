## Introduction

Streamliner 5+ supports schema evolution. At the current time it only supports adding columns to tables but will support backwards compatible expansion of columns in the future.

This supports customers the following use case:

![Schema Evolution Diagram](../images/schema-evolution.png)

## Order of Operations

When performing schema evolution, careful ordering of operations is important to ensure that no data is lost in the schema evolution process.

For example, if a column is added and data is inserted into the new column before the downstream scripts are updated, those values will not be populated in Snowflake and an eventual reload will be required. In order to illustrate this point, assume we are loading CSV files to a table:

```sql
create or replace t1 (
C1 NUMBER(38,8),
C2 VARCHAR(300),
C3 VARCHAR(50)
);
```

If the source system defines a new column `C4` and starts populating before Streamliner is updated, Streamliner will run the following:

```sql
COPY INTO ...  (C1,C2,C3)
FROM ( SELECT $1::NUMBER(38, 8),
$2::VARCHAR(300),
$3::VARCHAR(50)
```

On this example CSV file:

```
1.0,r1c2,r1c3,r1c4
2.0,r2c2,r2c3,r2c4
3.0,r3c2,r3c3,r3c4
```

Which will complete without error, and the data in `C4` will be lost.

Conversely, if files exist in the stage without the new columns, referencing them in a `COPY INTO … SELECT` will not result in an error. For example, if we have the following statement for mapping a CSV file to a table:

```sql
COPY INTO ...  (C1,C2,C3,C4)
FROM ( SELECT $1::NUMBER(38, 8),
$2::VARCHAR(300),
$3::VARCHAR(50),
$4::VARCHAR(50)
```

And the following CSV file which does not have four fields:

```
1.0,r1c2,r1c3
1.0,r2c2,r2c3
1.0,r3c2,r3c3
```

The COPY INTO will complete successfully.

Therefore the correct order of operations is as follows:

1. Column added to source database.
2. Streamliner schema evolution is executed
   1. Adds columns to tables
   2. Redefines copy-into statements
3. Column in source system begins to be populated

Note that it's also important for a PIPE to be empty when it's redefined. Therefore, Streamliner takes care to PAUSE and then wait for it to empty before redefining it. You can see this in the create-snowpipe-schema-evolution template file.

## Commands

Let's say we have tables t1, t2 and t3 where t1 have column c1 and c2 in oracle and we want to get the metadata of these tables and generates scripts that
can be executed in Snowflake to create these tables. To achieve this below commands should be executed sequentially.

1. Schema command to extract oracle database schema.

```shell script
./bin/streamliner schema --config conf/private-ingest-configuration.yml --state-directory output/state-directory --database-password <db_pass>
```

Note : Before executing above command make sure _state-directory_ is a directory.

Output of this command is a _state-directory_ that contains various config files. Each file contains config per table. In our case t1.yml, t2.yml and t3.yml will be generated that contains the table and table columns details.

2. Scripts command to generate initial scripts.

```shell script
./bin/streamliner scripts --config conf/private-ingest-configuration.yml --state-directory output/state-directory --previous-state-directory output/previous-state-directory --template-directory templates/snowflake --type-mapping conf/type-mapping.yml --output-path output/scripts
```

Output of step 1 _state-directory_ is passed to _--state-directory_ in this command.

Output of this command is
1. scripts that can be executed in snowflake to create table t1, t2 and t3.
2. folder _previous-state-directory_ is created.  
3. on successful execution of all the scripts of a table, table config file (example t1.yml) is moved from _state-directory_ to _previous-state-directory_. 

As time passes a new table t4 is created in oracle schema. Also a new column c3 is added in table t1. Now to keep snowflake updated with oracle schema we have to execute below commands.

3. Schema command to extract oracle schema and calculate difference.

```shell script
./bin/streamliner schema --config conf/private-ingest-configuration.yml --state-directory output/state-directory --previous-state-directory  output/previous-state-directory --database-password <db_pass>
```

Note : Before executing above command make sure _state-directory_ and _previous-state-directory_ is a directory.

Output of step 2 _previous-state-directory_ folder is passed to _--previous-state-directory_ in this command to calculate difference.

Output of this command is : 
   * state-directory: This folder contains various config files. Each file contains config per table. This folder will also have t4.yml that stores new table t4 details and t1.yml will have new column c3 details as well.
   * streamliner-diff.yml: This contains details of difference between oracle schema states. In this case it will have new table t4 and column c3 in table t1 details. By default **streamliner-diff.yml** is generated in _state-directory_ folder

4. Scripts command to generate evolve schema scripts. 

```shell script
./bin/streamliner scripts --config conf/private-ingest-configuration.yml --state-directory output/state-directory --previous-state-directory output/previous-state-directory --template-directory templates/snowflake --type-mapping conf/type-mapping.yml --output-path output/scripts
```

Output of step 3 _state-directory_ is passed to  _--state-directory_  and output of step 2 _previous-state-directory_ is passed to _--previous-state-directory_ in this command.

Output of this command is 
1. scripts that can be executed in snowflake to create table 4 and alter table t1 to add column c3.
2. on successful execution of all the scripts of a table, table config file (example t4.yml) is moved from _state-directory_ to _previous-state-directory_.

## Exception Handling Process

Streamliner handles the following changes:

* New columns
* Compatible extension of columns such as extending a string type
* Modifying comments
* Modifying nullability

Streamliner is unable to handle column renames or column deletes. These will need to be handled via an exception process.

The risks and process associated with handling exceptions varies by data format. Snowflake loads CSV data positionally and therefore more likely to be corrupt. **Snowflake loads Parquet data by source column name. Parquet makes this exception process significantly easier and therefore is strongly recommended.**

### Column Renames

The safest solution is to drop and reload the table.

#### Parquet

If no data has been loaded since the column rename, then the new data will show up with the new column name, and it's safe to follow the following process:

1. Rename column on the Snowflake side.
2. Re-define the PIPE object to use the new column name.

#### CSV

There are no guarantees about the ordinal position of the column. The table should be reloaded.

### Column Deletes

Column deletes are simpler to handle, especially with Parquet.

#### Parquet

Column deletes are handled gracefully. If the column has yet to be deleted in Snowflake, it'll be populated with null. Therefore, you simply need to:

1. Drop column on the Snowflake side.
2. Re-define the PIPE object to remove the use of the deleted column.
3. Move table config file from _state-directory_ to _previous-state-directory_.

#### CSV

CSV is complicated as Snowflake uses ordinal position to identify column. However, if no data has been loaded since the user deleted the column, you can:

1. Drop column on the Snowflake side.
2. Re-define the PIPE object to remove the use of the deleted column.
3. Move table config file from _state-directory_ to _previous-state-directory_.

If data has been loaded without the column, then the ordering is wrong and corrupt data has been loaded. The table should be reloaded.

### Column Extension

#### Both CSV and Parquet

Currently Streamliner supports column extension having data type equivalent to [Snowflake String data type](https://docs.snowflake.com/en/sql-reference/data-types-text.html) only.

For example: Oracle varchar2 data type is equivalent to Snowflake varchar data type. 

### Tables Delete

#### Both CSV and Parquet

Below manual steps are needed to handle table delete.

1. Drop the table on the Snowflake side.
2. Drop the PIPE object.
3. Delete the table config file from _previous-state-directory_. If file is not deleted, it will generate wrong difference output. 