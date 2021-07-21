snowsql-cmd=snowsql -c connection -f

evolve-schema: evolve-schema.sql
	$(snowsql-cmd) evolve-schema.sql

copy-into: copy-into.sql
	$(snowsql-cmd) copy-into.sql

snowpipe-redefinition: snowpipe-redefinition.sql
	$(snowsql-cmd) snowpipe-redefinition.sql

run:
	$(MAKE) evolve-schema
	$(MAKE) snowpipe-redefinition
	$(MAKE) copy-into