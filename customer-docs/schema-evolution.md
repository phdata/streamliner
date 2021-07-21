- [Introduction](#introduction)
- [Order of Operations](#order-of-operations)
- [Commands](#commands)

# Introduction

Streamliner 5+ supports schema evolution. At the current time it only supports adding columns to tables but will support backwards compatible expansion of columns in the future.

This supports customers the following use case:

![Schema Evolution Diagram](../images/schema-evolution.png)

# Order of Operations

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

# Commands

TODO Write the exact sequence of commands which need to be executed in an example situation.
