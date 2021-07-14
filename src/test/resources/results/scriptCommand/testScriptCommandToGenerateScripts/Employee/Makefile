


snowsql-cmd=snowsql -c connection -f

copy-into: copy-into.sql
	$(snowsql-cmd) copy-into.sql

create-schema: create-schema.sql
	$(snowsql-cmd) create-schema.sql

create-snowpipe: create-snowpipe.sql
	$(snowsql-cmd) create-snowpipe.sql

create-stage: create-stage.sql
	$(snowsql-cmd) create-stage.sql

create-table: create-table.sql
	$(snowsql-cmd) create-table.sql

drop-schema: drop-schema.sql
	$(snowsql-cmd) drop-schema.sql

drop-snowpipe: drop-snowpipe.sql
	$(snowsql-cmd) drop-snowpipe.sql

drop-stage: drop-stage.sql
	$(snowsql-cmd) drop-stage.sql

drop-table: drop-table.sql
	$(snowsql-cmd) drop-table.sql

first-run:
	$(MAKE) create-schema
	$(MAKE) create-stage
	$(MAKE) create-table
	$(MAKE) copy-into
	$(MAKE) create-snowpipe

drop:
	$(MAKE) drop-table
	$(MAKE) drop-snowpipe


