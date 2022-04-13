select TABLE_SCHEMA, TABLE_NAME,TABLE_TYPE, COMMENT from information_schema.tables
where {{TABLE_FILTER}}
 UPPER(table_schema) = UPPER('{{SCHEMA_NAME}}');
