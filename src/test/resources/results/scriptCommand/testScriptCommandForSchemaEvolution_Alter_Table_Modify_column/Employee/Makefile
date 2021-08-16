snowsql-cmd=snowsql -c connection -f

create-table-evolve-schema: create-table-evolve-schema.sql
	$(snowsql-cmd) create-table-evolve-schema.sql

copy-into: copy-into.sql
	$(snowsql-cmd) copy-into.sql

create-snowpipe: create-snowpipe.sql
	$(snowsql-cmd) create-snowpipe.sql

alter-snowpipe: alter-snowpipe.sql
	$(snowsql-cmd) alter-snowpipe.sql

no-change: no-change.sql
	$(snowsql-cmd) no-change.sql

evolve-schema:
	$(MAKE) create-table-evolve-schema


	
	$(MAKE) no-change