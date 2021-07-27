snowsql-cmd=snowsql -c connection -f

create-table-evolve-schema: create-table-evolve-schema.sql
	$(snowsql-cmd) create-table-evolve-schema.sql

copy-into: copy-into.sql
	$(snowsql-cmd) copy-into.sql

create-snowpipe-evolve-schema: create-snowpipe-evolve-schema.sql
	$(snowsql-cmd) create-snowpipe-evolve-schema.sql

first-run:
	$(MAKE) create-table-evolve-schema
	$(MAKE) create-snowpipe-evolve-schema
	$(MAKE) copy-into

run:
	$(MAKE) create-table-evolve-schema
	$(MAKE) create-snowpipe-evolve-schema